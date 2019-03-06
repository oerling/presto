/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.orc.reader;

import com.facebook.presto.memory.context.AggregatedMemoryContext;
import com.facebook.presto.orc.Filter;
import com.facebook.presto.orc.Filters;
import com.facebook.presto.orc.OrcCorruptionException;
import com.facebook.presto.orc.QualifyingSet;
import com.facebook.presto.orc.StreamDescriptor;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.stream.BooleanInputStream;
import com.facebook.presto.orc.stream.InputStreamSource;
import com.facebook.presto.orc.stream.InputStreamSources;
import com.facebook.presto.orc.stream.LongInputStream;
import com.facebook.presto.spi.PageSourceOptions.ErrorSet;
import com.facebook.presto.spi.SubfieldPath;
import com.facebook.presto.spi.SubfieldPath.PathElement;
import com.facebook.presto.spi.block.ArrayBlock;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.google.common.io.Closer;
import org.joda.time.DateTimeZone;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.orc.metadata.Stream.StreamKind.LENGTH;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.PRESENT;
import static com.facebook.presto.orc.reader.StreamReaders.createStreamReader;
import static com.facebook.presto.orc.stream.MissingInputStreamSource.missingStreamSource;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Verify.verify;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class ListStreamReader
        extends RepeatedColumnReader
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ListStreamReader.class).instanceSize();

    private final StreamDescriptor streamDescriptor;

    private final StreamReader elementStreamReader;

    private int readOffset;
    private int nextBatchSize;

    private InputStreamSource<BooleanInputStream> presentStreamSource = missingStreamSource(BooleanInputStream.class);

    private InputStreamSource<LongInputStream> lengthStreamSource = missingStreamSource(LongInputStream.class);
    @Nullable
    private LongInputStream lengthStream;

    // The set of subscripts for which data needs to be
    // returned. Other positions can be initialized to null. If this
    // is null, values for all subscripts must be returned.
    long[] subscripts;

    HashMap<Long, Filter> subscriptToFilter;
    Filters.PositionalFilter positionalFilter;
    boolean filterIsSetup;
    Filter[] elementFilters;
    // For each array in the inputQualifyingSet, the number of element filters that fit.
    int[] numElementFilters;
    // Count of elements at the beginning of current call to scan().
    int initialNumElements;

    public ListStreamReader(StreamDescriptor streamDescriptor, DateTimeZone hiveStorageTimeZone, AggregatedMemoryContext systemMemoryContext)
    {
        this.streamDescriptor = requireNonNull(streamDescriptor, "stream is null");
        this.elementStreamReader = createStreamReader(streamDescriptor.getNestedStreams().get(0), hiveStorageTimeZone, systemMemoryContext);
    }

    @Override
    public void setReferencedSubfields(List<SubfieldPath> subfields, int depth)
    {
        HashSet<Long> referencedSubscripts = new HashSet();
        boolean mayPruneElement = true;
        ArrayList<SubfieldPath> pathsForElement = new ArrayList();
        for (SubfieldPath subfield : subfields) {
            List<PathElement> pathElements = subfield.getPath();
            PathElement subscript = pathElements.get(depth + 1);
            if (!subscript.getIsSubscript()) {
                throw new IllegalArgumentException("List reader needs a PathElement with a subscript");
            }
            if (subscript.getSubscript() == PathElement.allSubscripts) {
                referencedSubscripts = null;
            }
            else {
                referencedSubscripts.add(subscript.getSubscript() - 1);
            }
            if (pathElements.size() > depth + 1) {
                pathsForElement.add(subfield);
            }
            else {
                mayPruneElement = false;
            }
        }
        if (mayPruneElement) {
            elementStreamReader.setReferencedSubfields(pathsForElement, depth + 1);
        }
        if (referencedSubscripts != null) {
            subscripts = referencedSubscripts.stream().mapToLong(Long::longValue).toArray();
            Arrays.sort(subscripts);
        }
    }

    @Override
    public void prepareNextRead(int batchSize)
    {
        readOffset += nextBatchSize;
        nextBatchSize = batchSize;
    }

    @Override
    public Block readBlock(Type type)
            throws IOException
    {
        if (!rowGroupOpen) {
            openRowGroup();
        }

        if (readOffset > 0) {
            if (presentStream != null) {
                // skip ahead the present bit reader, but count the set bits
                // and use this as the skip size for the data reader
                readOffset = presentStream.countBitsSet(readOffset);
            }
            if (readOffset > 0) {
                if (lengthStream == null) {
                    throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Value is not null but data stream is not present");
                }
                long elementSkipSize = lengthStream.sum(readOffset);
                elementStreamReader.prepareNextRead(toIntExact(elementSkipSize));
            }
        }

        // We will use the offsetVector as the buffer to read the length values from lengthStream,
        // and the length values will be converted in-place to an offset vector.
        int[] offsetVector = new int[nextBatchSize + 1];
        boolean[] nullVector = null;
        if (presentStream == null) {
            if (lengthStream == null) {
                throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Value is not null but data stream is not present");
            }
            lengthStream.nextIntVector(nextBatchSize, offsetVector, 0);
        }
        else {
            nullVector = new boolean[nextBatchSize];
            int nullValues = presentStream.getUnsetBits(nextBatchSize, nullVector);
            if (nullValues != nextBatchSize) {
                if (lengthStream == null) {
                    throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Value is not null but data stream is not present");
                }
                lengthStream.nextIntVector(nextBatchSize, offsetVector, 0, nullVector);
            }
        }

        // Convert the length values in the offsetVector to offset values in place
        int currentLength = offsetVector[0];
        offsetVector[0] = 0;
        for (int i = 1; i < offsetVector.length; i++) {
            int nextLength = offsetVector[i];
            offsetVector[i] = offsetVector[i - 1] + currentLength;
            currentLength = nextLength;
        }

        Type elementType = type.getTypeParameters().get(0);
        int elementCount = offsetVector[offsetVector.length - 1];

        Block elements;
        if (elementCount > 0) {
            elementStreamReader.prepareNextRead(elementCount);
            elements = elementStreamReader.readBlock(elementType);
        }
        else {
            elements = elementType.createBlockBuilder(null, 0).build();
        }
        Block arrayBlock = ArrayBlock.fromElementBlock(nextBatchSize, Optional.ofNullable(nullVector), offsetVector, elements);

        readOffset = 0;
        nextBatchSize = 0;

        return arrayBlock;
    }

    protected void openRowGroup()
            throws IOException
    {
        presentStream = presentStreamSource.openStream();
        lengthStream = lengthStreamSource.openStream();
        super.openRowGroup();
    }

    @Override
    public void startStripe(InputStreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
            throws IOException
    {
        presentStreamSource = missingStreamSource(BooleanInputStream.class);
        lengthStreamSource = missingStreamSource(LongInputStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;
        lengthStream = null;

        rowGroupOpen = false;

        elementStreamReader.startStripe(dictionaryStreamSources, encoding);
    }

    @Override
    public void startRowGroup(InputStreamSources dataStreamSources)
            throws IOException
    {
        presentStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, PRESENT, BooleanInputStream.class);
        lengthStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, LENGTH, LongInputStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;
        lengthStream = null;

        rowGroupOpen = false;

        elementStreamReader.startRowGroup(dataStreamSources);
    }

    @Override
    protected void eraseContent(int innerEnd)
    {elementStreamReader.erase(innerEnd);
    }
    @Override
    protected void compactContent(int[] innerSurviving, int innerSurvivingDase, int numInnerSurviving)
    {
        elementStreamReader.compactValues(innerSurviving, innerSurvivingBase, numInnerSurviving);
    }

    @Override
    public int getResultSizeInBytes()
    {
        if (outputChannel == -1) {
            return 0;
        }
        return elementStreamReader.getResultSizeInBytes();
    }

    public int getAverageResultSize()
    {
        return (int) (1 + (elementStreamReader.getAverageResultSize() * innerRowCount / (1 + outerRowCount)));
    }

    @Override
    public void setResultSizeBudget(long bytes)
    {
        elementStreamReader.setResultSizeBudget(bytes);
    }

    private void setupFilterAndChannel()
    {
        Filter elementFilter = null;
        if (filter != null) {
            Filters.StructFilter listFilter = (Filters.StructFilter) filter;
            HashMap<PathElement, Filter> filters = listFilter.getFilters();
            for (Map.Entry<PathElement, Filter> entry : filters.entrySet()) {
                verify(entry.getKey().getIsSubscript());
                long subscript = entry.getKey().getSubscript();
                if (subscript == PathElement.allSubscripts) {
                    verify(filters.size() == 1, "Only one elementFilter is allowed if no subscript is given");
                    elementFilter = entry.getValue();
                    break;
                }
                else {
                    if (subscriptToFilter == null) {
                        subscriptToFilter = new HashMap();
                        positionalFilter = new Filters.PositionalFilter();
                        elementFilter = positionalFilter;
                    }
                    subscriptToFilter.put(new Long(subscript - 1), entry.getValue());
                }
            }
        }
        elementStreamReader.setFilterAndChannel(elementFilter, outputChannel, -1, type.getTypeParameters().get(0));
        filterIsSetup = true;
    }

    private void setupPositionalFilter()
    {
        if (numElementFilters == null || numElementFilters.length < inputQualifyingSet.getPositionCount()) {
            numElementFilters = new int[inputQualifyingSet.getPositionCount()];
        }
        Arrays.fill(numElementFilters, 0, inputQualifyingSet.getPositionCount(), 0);
        if (elementFilters == null || elementFilters.length < innerQualifyingSet.getPositionCount()) {
            elementFilters = new Filter[innerQualifyingSet.getPositionCount()];
        }
        else {
            Arrays.fill(elementFilters, 0, innerQualifyingSet.getPositionCount(), null);
        }
        int innerStart = 0;
        int numInput = inputQualifyingSet.getPositionCount();
        for (int i = 0; i < numInput; i++) {
            int length = elementLength[i];
            int filterCount = 0;
            for (Map.Entry<Long, Filter> entry : subscriptToFilter.entrySet()) {
                int subscript = toIntExact(entry.getKey().longValue());
                if (subscript < length) {
                    elementFilters[innerStart + subscript] = entry.getValue();
                    filterCount++;
                }
                numElementFilters[i] = filterCount;
            }
            innerStart += length;
        }
        positionalFilter.setFilters(innerQualifyingSet, elementFilters);
    }

    @Override
    public void scan()
            throws IOException
    {
        if (!filterIsSetup) {
            setupFilterAndChannel();
        }
        if (!rowGroupOpen) {
            openRowGroup();
        }
        beginScan(presentStream, lengthStream);
        initialNumElements = elementStreamReader.getNumValues();
        if (numValues == 771) {
            System.out.println("1");
        }
        makeInnerQualifyingSet();
        if (positionalFilter != null) {
            setupPositionalFilter();
        }
        if (innerQualifyingSet.getPositionCount() > 0) {
            elementStreamReader.setInputQualifyingSet(innerQualifyingSet);
            elementStreamReader.scan();
            innerPosInRowGroup = innerQualifyingSet.getEnd();
        }
        else {
            elementStreamReader.getOrCreateOutputQualifyingSet().reset(0);
        }
        ensureValuesCapacity(inputQualifyingSet.getPositionCount());
        int lastElementOffset = numValues == 0 ? 0 : elementOffset[numValues];
        int numInput = inputQualifyingSet.getPositionCount();
        if (filter != null) {
            QualifyingSet filterResult = elementStreamReader.getOutputQualifyingSet();
            outputQualifyingSet.reset(inputQualifyingSet.getPositionCount());
            int numElementResults = filterResult.getPositionCount();
            int[] resultInputNumbers = filterResult.getInputNumbers();
            int[] resultRows = filterResult.getPositions();
            if (innerSurviving == null || innerSurviving.length < numElementResults) {
                innerSurviving = new int[numElementResults];
            }
            numInnerSurviving = 0;
            int outputIdx = 0;
            numInnerResults = 0;
            for (int i = 0; i < numInput; i++) {
                outputIdx = processFilterHits(i, outputIdx, resultRows, resultInputNumbers, numElementResults);
            }
            elementStreamReader.compactValues(innerSurviving, initialNumElements, numInnerSurviving);
            if (numValues == 771) {
                System.out.println("2");
            }
        }
        else {
            numInnerResults = inputQualifyingSet.getPositionCount() - numNullsToAdd;
        }
        addNullsAfterScan(filter != null ? outputQualifyingSet : inputQualifyingSet, inputQualifyingSet.getEnd());
        if (filter == null) {
            // The lengths are unchanged.
            int valueIdx = numValues;
            for (int i = 0; i < numInput; i++) {
                elementOffset[valueIdx] = lastElementOffset;
                lastElementOffset += elementLength[i];
                valueIdx++;
            }
            elementOffset[valueIdx] = lastElementOffset;
        }
        else {
            if (numNullsToAdd > 0 && outputChannel != -1) {
                // There was a filter and nulls were added by
                // addNullsAfterScan(). Fill null positions in
                // elementOffset with the offset of the next non-null.
                elementOffset[numValues + numResults] = lastElementOffset;
                int nextNonNull = lastElementOffset;
                for (int i = numValues + numResults - 1; i >= numValues; i--) {
                    if (elementOffset[i] == -1) {
                        elementOffset[i] = nextNonNull;
                    }
                    else {
                        nextNonNull = elementOffset[i];
                    }
                }
            }
            }
        endScan(presentStream);
    }

    // Counts how many hits one array has. Adds the array to surviving
    // if all hit and all filters existed. Adds array to errors if all
    // hit but not all subscripts existed. Else the array did not
    // pass. Returns the index to the first element of the next array
    // in the inner outputQualifyingSet.
    int processFilterHits(int inputIdx, int outputIdx, int[] resultRows, int[] resultInputNumbers, int numElementResults)
    {
        int filterHits = 0;
        int count = 0;
        int initialOutputIdx = outputIdx;
        if (presentStream != null && !present[inputQualifyingSet.getPositions()[inputIdx]]) {
            return outputIdx;
        }
        int[] inputNumbers = innerQualifyingSet.getInputNumbers(); 
        // Count rows and filter hits from the array corresponding to inputIdx.
        while (outputIdx < numElementResults && inputNumbers[resultInputNumbers[outputIdx]] == inputIdx) {
            count++;
            int posInArray = resultRows[outputIdx] - elementStart[inputIdx];
            if (subscriptToFilter.get(Long.valueOf(posInArray)) != null) {
                filterHits++;
            }
            outputIdx++;
        }
        if (filterHits < numElementFilters[inputIdx]) {
            // Some filter did not hit.
            return outputIdx;
        }
        outputQualifyingSet.append(inputQualifyingSet.getPositions()[inputIdx], inputIdx);
        addArrayToResult(inputIdx, initialOutputIdx, outputIdx);
        if (numElementFilters[inputIdx] < subscriptToFilter.size()) {
            ErrorSet errorSet = outputQualifyingSet.getOrCreateErrorSet();
            errorSet.addError(outputQualifyingSet.getPositionCount() - 1, inputQualifyingSet.getPositionCount(), new IllegalArgumentException("List subscript out of bounds"));
        }
        return outputIdx;
    }

    private void addArrayToResult(int inputIdx, int beginResult, int endResult)
    {
        if (outputChannel == -1) {
            return;
        }
        elementOffset[numValues + numInnerResults] = numInnerSurviving + initialNumElements;
        for (int i = beginResult; i < endResult; i++) {
            innerSurviving[numInnerSurviving++] = i;
        }
        elementOffset[numValues + numInnerResults + 1] = numInnerSurviving + initialNumElements;
        numInnerResults++;
    }

    @Override
    public Block getBlock(int numFirstRows, boolean mayReuse)
    {
        int innerFirstRows = getInnerPosition(numFirstRows);
            int[] offsets = mayReuse ? elementOffset : Arrays.copyOf(elementOffset, numFirstRows + 1);
        boolean[] nulls = valueIsNull == null ? null
            : mayReuse ? valueIsNull : Arrays.copyOf(valueIsNull, numFirstRows);
        Block elements;
        if (innerFirstRows == 0) {
            Type elementType = type.getTypeParameters().get(0);
            elements = elementType.createBlockBuilder(null, 0).build();
        }
        else {
            elements = elementStreamReader.getBlock(innerFirstRows, mayReuse);
        }
        return ArrayBlock.fromElementBlock(numFirstRows, Optional.ofNullable(nulls), offsets, elements);
    }

    @Override
    protected void shiftUp(int from, int to)
    {
        elementOffset[to] = elementOffset[from];
    }

    @Override
    protected void writeNull(int position)
    {
        elementOffset[position] = -1;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(streamDescriptor)
                .toString();
    }

    @Override
    public void close()
    {
        try (Closer closer = Closer.create()) {
            closer.register(() -> elementStreamReader.close());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + elementStreamReader.getRetainedSizeInBytes();
    }
}
