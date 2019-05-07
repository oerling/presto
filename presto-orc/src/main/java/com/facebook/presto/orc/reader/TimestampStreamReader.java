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

import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.orc.Filter;
import com.facebook.presto.orc.OrcCorruptionException;
import com.facebook.presto.orc.QualifyingSet;
import com.facebook.presto.orc.StreamDescriptor;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.stream.BooleanInputStream;
import com.facebook.presto.orc.stream.InputStreamSource;
import com.facebook.presto.orc.stream.InputStreamSources;
import com.facebook.presto.orc.stream.LongInputStream;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.LongArrayBlock;
import com.facebook.presto.spi.type.Type;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.facebook.presto.orc.ResizedArrays.resize;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.DATA;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.PRESENT;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.SECONDARY;
import static com.facebook.presto.orc.stream.MissingInputStreamSource.missingStreamSource;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static java.util.Objects.requireNonNull;

public class TimestampStreamReader
        extends ColumnReader
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(TimestampStreamReader.class).instanceSize();

    private static final int MILLIS_PER_SECOND = 1000;

    private final StreamDescriptor streamDescriptor;
    private final long baseTimestampInSeconds;

    private int readOffset;
    private int nextBatchSize;

    private InputStreamSource<LongInputStream> secondsStreamSource = missingStreamSource(LongInputStream.class);
    @Nullable
    private LongInputStream secondsStream;

    private InputStreamSource<LongInputStream> nanosStreamSource = missingStreamSource(LongInputStream.class);
    @Nullable
    private LongInputStream nanosStream;

    private LocalMemoryContext systemMemoryContext;

    private long[] values;

    public TimestampStreamReader(StreamDescriptor streamDescriptor, DateTimeZone hiveStorageTimeZone, LocalMemoryContext systemMemoryContext)
    {
        super(OptionalInt.of(SIZE_OF_LONG));
        this.streamDescriptor = requireNonNull(streamDescriptor, "stream is null");
        this.baseTimestampInSeconds = new DateTime(2015, 1, 1, 0, 0, requireNonNull(hiveStorageTimeZone, "hiveStorageTimeZone is null")).getMillis() / MILLIS_PER_SECOND;
        this.systemMemoryContext = requireNonNull(systemMemoryContext, "systemMemoryContext is null");
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
                if (secondsStream == null) {
                    throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Value is not null but seconds stream is not present");
                }
                if (nanosStream == null) {
                    throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Value is not null but nanos stream is not present");
                }

                secondsStream.skip(readOffset);
                nanosStream.skip(readOffset);
            }
        }

        BlockBuilder builder = type.createBlockBuilder(null, nextBatchSize);

        if (presentStream == null) {
            if (secondsStream == null) {
                throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Value is not null but seconds stream is not present");
            }
            if (nanosStream == null) {
                throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Value is not null but nanos stream is not present");
            }

            for (int i = 0; i < nextBatchSize; i++) {
                type.writeLong(builder, decodeTimestamp(secondsStream.next(), nanosStream.next(), baseTimestampInSeconds));
            }
        }
        else {
            for (int i = 0; i < nextBatchSize; i++) {
                if (presentStream.nextBit()) {
                    verify(secondsStream != null, "Value is not null but seconds stream is not present");
                    verify(nanosStream != null, "Value is not null but nanos stream is not present");
                    type.writeLong(builder, decodeTimestamp(secondsStream.next(), nanosStream.next(), baseTimestampInSeconds));
                }
                else {
                    builder.appendNull();
                }
            }
        }

        readOffset = 0;
        nextBatchSize = 0;
        return builder.build();
    }

    @Override
    protected void openRowGroup()
            throws IOException
    {
        presentStream = presentStreamSource.openStream();
        secondsStream = secondsStreamSource.openStream();
        nanosStream = nanosStreamSource.openStream();
        super.openRowGroup();
    }

    @Override
    public void startStripe(InputStreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
    {
        presentStreamSource = missingStreamSource(BooleanInputStream.class);
        secondsStreamSource = missingStreamSource(LongInputStream.class);
        nanosStreamSource = missingStreamSource(LongInputStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;
        secondsStream = null;
        nanosStream = null;

        rowGroupOpen = false;
    }

    @Override
    public void startRowGroup(InputStreamSources dataStreamSources)
    {
        presentStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, PRESENT, BooleanInputStream.class);
        secondsStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, DATA, LongInputStream.class);
        nanosStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, SECONDARY, LongInputStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;
        secondsStream = null;
        nanosStream = null;

        rowGroupOpen = false;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(streamDescriptor)
                .toString();
    }

    // This comes from the Apache Hive ORC code
    public static long decodeTimestamp(long seconds, long serializedNanos, long baseTimestampInSeconds)
    {
        long millis = (seconds + baseTimestampInSeconds) * MILLIS_PER_SECOND;
        long nanos = parseNanos(serializedNanos);
        if (nanos > 999999999 || nanos < 0) {
            throw new IllegalArgumentException("nanos field of an encoded timestamp in ORC must be between 0 and 999999999 inclusive, got " + nanos);
        }

        // the rounding error exists because java always rounds up when dividing integers
        // -42001/1000 = -42; and -42001 % 1000 = -1 (+ 1000)
        // to get the correct value we need
        // (-42 - 1)*1000 + 999 = -42001
        // (42)*1000 + 1 = 42001
        if (millis < 0 && nanos != 0) {
            millis -= 1000;
        }
        // Truncate nanos to millis and add to mills
        return millis + (nanos / 1_000_000);
    }

    // This comes from the Apache Hive ORC code
    private static int parseNanos(long serialized)
    {
        int zeros = ((int) serialized) & 0b111;
        int result = (int) (serialized >>> 3);
        if (zeros != 0) {
            for (int i = 0; i <= zeros; ++i) {
                result *= 10;
            }
        }
        return result;
    }

    @Override
    public void close()
    {
        systemMemoryContext.close();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE;
    }

    @Override
    public void setFilterAndChannel(Filter filter, int channel, int columnIndex, Type type)
    {
        checkArgument(type == TIMESTAMP, "Unsupported type: " + type.getDisplayName());
        super.setFilterAndChannel(filter, channel, columnIndex, type);
    }

    @Override
    public void scan()
            throws IOException
    {
        if (!rowGroupOpen) {
            openRowGroup();
        }
        beginScan(presentStream, null);
        QualifyingSet input = inputQualifyingSet;
        QualifyingSet output = outputQualifyingSet;
        int end = input.getEnd();
        int rowsInRange = end - posInRowGroup;
        int[] inputPositions = input.getPositions();
        ensureValuesCapacity(numValues + inputQualifyingSet.getPositionCount(), false);

        if (secondsStream == null) {
            processAllNulls();
            endScan(presentStream);
            return;
        }

        int nextActive = input.getPositionCount() > 0 ? inputPositions[0] : -1;
        int activeIdx = 0;
        int toSkip = 0;
        for (int i = 0; i < rowsInRange; i++) {
            if (i + posInRowGroup == nextActive) {
                if (presentStream != null && !present[i]) {
                    if (filter == null || filter.testNull()) {
                        if (filter != null) {
                            output.append(i + posInRowGroup, activeIdx);
                        }
                        addNullResult();
                    }
                }
                else {
                    // Non-null row in qualifying set.
                    if (toSkip > 0) {
                        secondsStream.skip(toSkip);
                        nanosStream.skip(toSkip);
                        toSkip = 0;
                    }
                    long value = decodeTimestamp(secondsStream.next(), nanosStream.next(), baseTimestampInSeconds);
                    if (filter != null) {
                        if (filter.testLong(value)) {
                            output.append(i + posInRowGroup, activeIdx);
                            addResult(value);
                        }
                    }
                    else {
                        // No filter.
                        addResult(value);
                    }
                }
                if (++activeIdx == input.getPositionCount()) {
                    toSkip = countPresent(i + 1, end - posInRowGroup);
                    break;
                }
                nextActive = inputPositions[activeIdx];
                if (outputChannelSet && numResults * SIZE_OF_LONG > resultSizeBudget) {
                    throw batchTooLarge();
                }
                continue;
            }
            else {
                // The row is not in the input qualifying set. Add to skip if non-null.
                if (presentStream == null || present[i]) {
                    toSkip++;
                }
            }
        }
        if (toSkip > 0) {
            secondsStream.skip(toSkip);
            nanosStream.skip(toSkip);
        }
        endScan(presentStream);
    }

    private void addResult(long value)
    {
        if (!outputChannelSet) {
            return;
        }

        int position = numValues + numResults;
        values[position] = value;
        if (valueIsNull != null) {
            valueIsNull[position] = false;
        }
        numResults++;
    }

    @Override
    protected void ensureValuesCapacity(int capacity, boolean includeNulls)
    {
        if (values == null || values.length < capacity) {
            values = resize(values, capacity);
            if (valueIsNull != null) {
                valueIsNull = resize(valueIsNull, capacity);
            }
        }
        if (includeNulls && valueIsNull == null) {
            valueIsNull = new boolean[values.length];
        }
    }

    @Override
    public Block getBlock(int numFirstRows, boolean mayReuse)
    {
        checkEnoughValues(numFirstRows);
        if (mayReuse) {
            return new LongArrayBlock(numFirstRows, valueIsNull == null ? Optional.empty() : Optional.of(valueIsNull), values);
        }
        if (numFirstRows < numValues || values.length > (int) (numFirstRows * 1.2)) {
            return new LongArrayBlock(numFirstRows,
                    valueIsNull == null ? Optional.empty() : Optional.of(Arrays.copyOf(valueIsNull, numFirstRows)),
                    Arrays.copyOf(values, numFirstRows));
        }
        Block block = new LongArrayBlock(numFirstRows, valueIsNull == null ? Optional.empty() : Optional.of(valueIsNull), values);
        values = null;
        valueIsNull = null;
        numValues = 0;
        return block;
    }

    @Override
    public void compactValues(int[] surviving, int base, int numSurviving)
    {
        if (outputChannelSet) {
            StreamReaders.compactArrays(surviving, base, numSurviving, values, valueIsNull);
            numValues = base + numSurviving;
        }
        compactQualifyingSet(surviving, numSurviving);
    }

    @Override
    public void erase(int numFirstRows)
    {
        if (values == null) {
            return;
        }
        numValues -= numFirstRows;
        if (numValues > 0) {
            System.arraycopy(values, numFirstRows, values, 0, numValues);
            if (valueIsNull != null) {
                System.arraycopy(valueIsNull, numFirstRows, valueIsNull, 0, numValues);
            }
        }
    }
}
