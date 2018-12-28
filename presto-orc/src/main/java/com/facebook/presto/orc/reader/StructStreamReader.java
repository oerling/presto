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
import com.facebook.presto.orc.ColumnGroupReader;
import com.facebook.presto.orc.Filter;
import com.facebook.presto.orc.Filters;
import com.facebook.presto.orc.QualifyingSet;
import com.facebook.presto.orc.StreamDescriptor;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.stream.BooleanInputStream;
import com.facebook.presto.orc.stream.InputStreamSource;
import com.facebook.presto.orc.stream.InputStreamSources;
import com.facebook.presto.spi.ReferencePath;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.RowBlock;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.Type;
import com.google.common.io.Closer;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.joda.time.DateTimeZone;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.orc.metadata.Stream.StreamKind.PRESENT;
import static com.facebook.presto.orc.reader.StreamReaders.createStreamReader;
import static com.facebook.presto.orc.stream.MissingInputStreamSource.missingStreamSource;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public class StructStreamReader
    extends ColumnReader
    implements StreamReader
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(StructStreamReader.class).instanceSize();

    private final StreamDescriptor streamDescriptor;

    private final Map<String, StreamReader> structFields;

    private int readOffset;
    private int nextBatchSize;

    private InputStreamSource<BooleanInputStream> presentStreamSource = missingStreamSource(BooleanInputStream.class);
    @Nullable
    private BooleanInputStream presentStream;

    ColumnGroupReader reader;
    int[] fieldBlockOffset;
    boolean[] valueIsNull;
    int[] fieldSurviving;
    QualifyingSet fieldQualifyingSet;
    // Passing rows of field filters are returned here, null if no field filters.
    QualifyingSet fieldOutputQualifyingSet;
    // Position in row group of first unprocessed field row.
    int posInFields;
    StreamReader[] streamReaders;
    // For each position in fieldQualifyingSet, the corresponding position in inputQualifyingSet.
    int[] innerToOuter;

    StructStreamReader(StreamDescriptor streamDescriptor, DateTimeZone hiveStorageTimeZone, AggregatedMemoryContext systemMemoryContext)
    {
        this.streamDescriptor = requireNonNull(streamDescriptor, "stream is null");
        this.structFields = streamDescriptor.getNestedStreams().stream()
                .collect(toImmutableMap(stream -> stream.getFieldName().toLowerCase(Locale.ENGLISH), stream -> createStreamReader(stream, hiveStorageTimeZone, systemMemoryContext)));
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
                // and use this as the skip size for the field readers
                readOffset = presentStream.countBitsSet(readOffset);
            }
            for (StreamReader structField : structFields.values()) {
                structField.prepareNextRead(readOffset);
            }
        }

        boolean[] nullVector = null;
        Block[] blocks;

        if (presentStream == null) {
            blocks = getBlocksForType(type, nextBatchSize);
        }
        else {
            nullVector = new boolean[nextBatchSize];
            int nullValues = presentStream.getUnsetBits(nextBatchSize, nullVector);
            if (nullValues != nextBatchSize) {
                blocks = getBlocksForType(type, nextBatchSize - nullValues);
            }
            else {
                List<Type> typeParameters = type.getTypeParameters();
                blocks = new Block[typeParameters.size()];
                for (int i = 0; i < typeParameters.size(); i++) {
                    blocks[i] = typeParameters.get(i).createBlockBuilder(null, 0).build();
                }
            }
        }

        verify(Arrays.stream(blocks)
                .mapToInt(Block::getPositionCount)
                .distinct()
                .count() == 1);

        // Struct is represented as a row block
        Block rowBlock = RowBlock.fromFieldBlocks(nextBatchSize, Optional.ofNullable(nullVector), blocks);

        readOffset = 0;
        nextBatchSize = 0;

        return rowBlock;
    }

    @Override
    void openRowGroup()
            throws IOException
    {
        presentStream = presentStreamSource.openStream();
        posInFields = 0;
        super.openRowGroup();
    }

    @Override
    public void startStripe(InputStreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
            throws IOException
    {
        presentStreamSource = missingStreamSource(BooleanInputStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;

        rowGroupOpen = false;

        for (StreamReader structField : structFields.values()) {
            structField.startStripe(dictionaryStreamSources, encoding);
        }
    }

    @Override
    public void startRowGroup(InputStreamSources dataStreamSources)
            throws IOException
    {
        presentStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, PRESENT, BooleanInputStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;

        rowGroupOpen = false;

        for (StreamReader structField : structFields.values()) {
            structField.startRowGroup(dataStreamSources);
        }
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(streamDescriptor)
                .toString();
    }

    private Block[] getBlocksForType(Type type, int positionCount)
            throws IOException
    {
        RowType rowType = (RowType) type;

        Block[] blocks = new Block[rowType.getFields().size()];

        for (int i = 0; i < rowType.getFields().size(); i++) {
            Optional<String> fieldName = rowType.getFields().get(i).getName();
            Type fieldType = rowType.getFields().get(i).getType();

            if (!fieldName.isPresent()) {
                throw new IllegalArgumentException("Missing struct field name in type " + rowType);
            }

            String lowerCaseFieldName = fieldName.get().toLowerCase(Locale.ENGLISH);
            StreamReader streamReader = structFields.get(lowerCaseFieldName);
            if (streamReader != null) {
                streamReader.prepareNextRead(positionCount);
                blocks[i] = streamReader.readBlock(fieldType);
            }
            else {
                blocks[i] = getNullBlock(fieldType, positionCount);
            }
        }
        return blocks;
    }

    private static Block getNullBlock(Type type, int positionCount)
    {
        Block nullValueBlock = type.createBlockBuilder(null, 1)
                .appendNull()
                .build();
        return new RunLengthEncodedBlock(nullValueBlock, positionCount);
    }

    @Override
    public void close()
    {
        try (Closer closer = Closer.create()) {
            for (StreamReader structField : structFields.values()) {
                closer.register(() -> structField.close());
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        long retainedSizeInBytes = INSTANCE_SIZE;
        for (StreamReader structField : structFields.values()) {
            retainedSizeInBytes += structField.getRetainedSizeInBytes();
        }
        return retainedSizeInBytes;
    }

    private void setupForScan ()
    {
        RowType rowType = (RowType) type;
        int numFields = rowType.getFields().size();
        int[] fieldColumns = new int[numFields];
        streamReaders = new StreamReader[numFields];
        HashMap<Integer, Filter> filters = new HashMap();
        for (int i = 0; i < numFields; i++) {
            fieldColumns[i] = i;
            Optional<String> fieldName = rowType.getFields().get(i).getName();
            Type fieldType = rowType.getFields().get(i).getType();

            if (!fieldName.isPresent()) {
                throw new IllegalArgumentException("Missing struct field name in type " + rowType);
            }

            if (filter != null) {
                Filters.StructFilter structFilter = (Filters.StructFilter) filter;
                Filter fieldFilter = structFilter.getMember(new ReferencePath.PathElement(fieldName.get(), 0));
                if (fieldFilter != null) {
                    filters.put(i, fieldFilter);
                }
            }
            String lowerCaseFieldName = fieldName.get().toLowerCase(Locale.ENGLISH);
            StreamReader streamReader = structFields.get(lowerCaseFieldName);
            if (streamReader != null) {
                streamReaders[i] = streamReader;
            }
        }
        reader = new ColumnGroupReader(streamReaders,
                                       null,
                                       fieldColumns,
                                       rowType.getTypeParameters(),
                                       fieldColumns,
                                       filters,
                                       true,
                                       true);
    }

    @Override
    public void setResultSizeBudget(int bytes)
    {
        if (reader == null) {
            setupForScan();
        }
        reader.setResultSizeBudget(bytes);
    }

    @Override
    public void erase(int end)
    {
        if (reader != null) {
            reader.newBatch(end);
        }
    }

    @Override
    public void compactValues(int[] surviving, int base, int numSurviving)
    {
        if (outputChannel != -1) {
            if (fieldSurviving == null || fieldSurviving.length < numSurviving) {
                fieldSurviving = new int[numSurviving];
            }
            int fieldBase = fieldBlockOffset[base];
            int numFieldSurviving = 0;
            for (int i = 0; i < numSurviving; i++) {
                if (valueIsNull != null && valueIsNull[base + surviving[i]]) {
                    valueIsNull[base + i] = true;
                    fieldBlockOffset[i] = fieldBase;
                }
                else {
                    fieldSurviving[numFieldSurviving++] = fieldBlockOffset[base + surviving[i]];
                    fieldBase++;
                    if (valueIsNull != null) {
                        valueIsNull[base + i] = false;
                    }
                    fieldBlockOffset[i] = fieldBase;
                }
            }
            for (StreamReader reader : streamReaders) {
                if (reader != null) {
                    reader.compactValues(fieldSurviving, base, numFieldSurviving);
                }
            }
            numValues = base + numSurviving;
        }
        compactQualifyingSet(surviving, numSurviving);
    }

    @Override
    public int getResultSizeInBytes()
    {
        if (reader == null) {
            return 0;
        }
        return reader.getResultSizeInBytes();
    }

    int innerDistance(int from, int to)
    {
        if (presentStream == null) {
            return to - from;
        }
        int distance = 0;
        for (int i = from; i < to; i++) {
            if (present[i - posInRowGroup]) {
                distance++;
            }
        }
        return distance;
    }
    
    @Override
    public void scan()
        throws IOException
    {
        beginScan(presentStream, null);
        QualifyingSet input = inputQualifyingSet;
        QualifyingSet output = outputQualifyingSet;
        // 
        IntArrayList nullResults = null;
        int numInput = input.getPositionCount();
        int[] inputRows = input.getPositions();
        int end = input.getEnd();
        int rowsInRange = end - posInRowGroup;
        int[] fieldRows = fieldQualifyingSet.getMutablePositions(numInput);
        int numFieldRows = 0;
        if (innerToOuter == null || innerToOuter.length < numInput) {
            innerToOuter = new int [numInput + 100];
        }
        int prevFieldRow = posInFields;
        int prevRow = posInRowGroup;
        for (int i = 0; i < numInput; i++) {
            int activeRow = inputRows[i]; 
            if (presentStream != null && !present[activeRow - posInRowGroup]) {
                // The struct is null.
                if (filter != null) {
                    if (filter.testNull()) {
                        nullResults.add(activeRow);
                    }
                }
            }
            else {
                int numSkip = innerDistance(prevRow, activeRow);
                fieldRows[numFieldRows] = prevFieldRow + numSkip;
                innerToOuter[numFieldRows] = activeRow;
                numFieldRows++;
                prevFieldRow += numSkip;
                fieldRows[numFieldRows] = posInFields + numFieldRows;
            }
            prevRow = activeRow;
        }
        int skip = innerDistance(prevRow, end);
        fieldQualifyingSet.setEnd(skip + prevFieldRow);
        fieldQualifyingSet.setPositionCount(numFieldRows);

        reader.setQualifyingSets(fieldQualifyingSet, fieldOutputQualifyingSet);
        reader.advance();
        int truncated = reader.getTruncationRow();
        if (truncated != -1) {
            posInFields = truncated;
            truncationRow = innerToOuterRow(truncated);
        }
        QualifyingSet inner = fieldOutputQualifyingSet;
    }

    int innerToOuterRow(int inner)
    {
        int numInner = fieldQualifyingSet.getPositionCount();
        int[] rows = fieldQualifyingSet.getPositions();
        for (int i = 0; i < numInner; i++) {
            if (inner == rows[i]) {
                return innerToOuter[i];
            }
        }
        throw new IllegalArgumentException("Can't translate from struct truncation row to enclosing truncation row");
    }

    @Override
    public void maybeReorderFilters()
    {
        reader.maybeReorderFilters();
    }
}
