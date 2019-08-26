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
package com.facebook.presto.operator;

import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.buffer.OutputBuffer;
import com.facebook.presto.execution.buffer.PagesSerde;
import com.facebook.presto.execution.buffer.PagesSerdeFactory;
import com.facebook.presto.execution.buffer.SerializedPage;
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.ArrayBlock;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockFlattener;
import com.facebook.presto.spi.block.BlockLease;
import com.facebook.presto.spi.block.ColumnarArray;
import com.facebook.presto.spi.block.ColumnarMap;
import com.facebook.presto.spi.block.ColumnarRow;
import com.facebook.presto.spi.block.DictionaryBlock;
import com.facebook.presto.spi.block.MapBlock;
import com.facebook.presto.spi.block.RowBlock;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Type;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SliceOutput;
import io.airlift.units.DataSize;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static com.facebook.presto.array.Arrays.ensureCapacity;
import static com.facebook.presto.operator.BlockEncodingBuffers.createBlockEncodingBuffers;
import static com.facebook.presto.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static com.facebook.presto.type.IntervalYearMonthType.INTERVAL_YEAR_MONTH;
import static com.facebook.presto.type.IpAddressType.IPADDRESS;
import static com.facebook.presto.type.UnknownType.UNKNOWN;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class OptimizedPartitionedOutputOperator
        implements Operator
{
    public static class OptimizedPartitionedOutputFactory
            implements OutputFactory
    {
        private final PartitionFunction partitionFunction;
        private final List<Integer> partitionChannels;
        private final List<Optional<ConstantExpression>> partitionConstants;
        private final OutputBuffer outputBuffer;
        private final boolean replicatesAnyRow;
        private final OptionalInt nullChannel;
        private final DataSize maxMemory;

        public OptimizedPartitionedOutputFactory(
                PartitionFunction partitionFunction,
                List<Integer> partitionChannels,
                List<Optional<ConstantExpression>> partitionConstants,
                boolean replicatesAnyRow,
                OptionalInt nullChannel,
                OutputBuffer outputBuffer,
                DataSize maxMemory)
        {
            this.partitionFunction = requireNonNull(partitionFunction, "pagePartitioner is null");
            this.partitionChannels = requireNonNull(partitionChannels, "partitionChannels is null");
            this.partitionConstants = requireNonNull(partitionConstants, "partitionConstants is null");
            this.replicatesAnyRow = replicatesAnyRow;
            this.nullChannel = requireNonNull(nullChannel, "nullChannel is null");
            this.outputBuffer = requireNonNull(outputBuffer, "outputBuffer is null");
            this.maxMemory = requireNonNull(maxMemory, "maxMemory is null");
        }

        @Override
        public OperatorFactory createOutputOperator(
                int operatorId,
                PlanNodeId planNodeId,
                List<Type> types,
                Function<Page, Page> pagePreprocessor,
                PagesSerdeFactory serdeFactory)
        {
            return new OptimizedPartitionedOutputOperatorFactory(
                    operatorId,
                    planNodeId,
                    types,
                    pagePreprocessor,
                    partitionFunction,
                    partitionChannels,
                    partitionConstants,
                    replicatesAnyRow,
                    nullChannel,
                    outputBuffer,
                    serdeFactory,
                    maxMemory);
        }
    }

    public static class OptimizedPartitionedOutputOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final List<Type> sourceTypes;
        private final Function<Page, Page> pagePreprocessor;
        private final PartitionFunction partitionFunction;
        private final List<Integer> partitionChannels;
        private final List<Optional<ConstantExpression>> partitionConstants;
        private final boolean replicatesAnyRow;
        private final OptionalInt nullChannel;
        private final OutputBuffer outputBuffer;
        private final PagesSerdeFactory serdeFactory;
        private final DataSize maxMemory;

        public OptimizedPartitionedOutputOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<Type> sourceTypes,
                Function<Page, Page> pagePreprocessor,
                PartitionFunction partitionFunction,
                List<Integer> partitionChannels,
                List<Optional<ConstantExpression>> partitionConstants,
                boolean replicatesAnyRow,
                OptionalInt nullChannel,
                OutputBuffer outputBuffer,
                PagesSerdeFactory serdeFactory,
                DataSize maxMemory)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.sourceTypes = requireNonNull(sourceTypes, "sourceTypes is null");
            this.pagePreprocessor = requireNonNull(pagePreprocessor, "pagePreprocessor is null");
            this.partitionFunction = requireNonNull(partitionFunction, "pagePartitioner is null");
            this.partitionChannels = requireNonNull(partitionChannels, "partitionChannels is null");
            this.partitionConstants = requireNonNull(partitionConstants, "partitionConstants is null");
            this.replicatesAnyRow = replicatesAnyRow;
            this.nullChannel = requireNonNull(nullChannel, "nullChannel is null");
            this.outputBuffer = requireNonNull(outputBuffer, "outputBuffer is null");
            this.serdeFactory = requireNonNull(serdeFactory, "serdeFactory is null");
            this.maxMemory = requireNonNull(maxMemory, "maxMemory is null");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, PartitionedOutputOperator.class.getSimpleName());
            return new OptimizedPartitionedOutputOperator(
                    operatorContext,
                    sourceTypes,
                    pagePreprocessor,
                    partitionFunction,
                    partitionChannels,
                    partitionConstants,
                    replicatesAnyRow,
                    nullChannel,
                    outputBuffer,
                    serdeFactory,
                    maxMemory);
        }

        @Override
        public void noMoreOperators()
        {
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new OptimizedPartitionedOutputOperatorFactory(
                    operatorId,
                    planNodeId,
                    sourceTypes,
                    pagePreprocessor,
                    partitionFunction,
                    partitionChannels,
                    partitionConstants,
                    replicatesAnyRow,
                    nullChannel,
                    outputBuffer,
                    serdeFactory,
                    maxMemory);
        }
    }

    private final OperatorContext operatorContext;
    private final Function<Page, Page> pagePreprocessor;
    private final PagePartitioner pagePartitioner;
    private final LocalMemoryContext systemMemoryContext;
    private boolean finished;

    public OptimizedPartitionedOutputOperator(
            OperatorContext operatorContext,
            List<Type> sourceTypes,
            Function<Page, Page> pagePreprocessor,
            PartitionFunction partitionFunction,
            List<Integer> partitionChannels,
            List<Optional<ConstantExpression>> partitionConstants,
            boolean replicatesAnyRow,
            OptionalInt nullChannel,
            OutputBuffer outputBuffer,
            PagesSerdeFactory serdeFactory,
            DataSize maxMemory)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.pagePreprocessor = requireNonNull(pagePreprocessor, "pagePreprocessor is null");
        this.pagePartitioner = new PagePartitioner(
                partitionFunction,
                partitionChannels,
                partitionConstants,
                replicatesAnyRow,
                nullChannel,
                outputBuffer,
                serdeFactory,
                sourceTypes,
                maxMemory,
                operatorContext.getDriverContext().getLifespan());

        operatorContext.setInfoSupplier(this::getInfo);
        this.systemMemoryContext = operatorContext.newLocalSystemMemoryContext(PartitionedOutputOperator.class.getSimpleName());
        this.systemMemoryContext.setBytes(pagePartitioner.getInitialRetainedSizeInBytes());
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    public PartitionedOutputInfo getInfo()
    {
        return pagePartitioner.getInfo();
    }

    @Override
    public void finish()
    {
        finished = true;
        pagePartitioner.flush();
    }

    @Override
    public boolean isFinished()
    {
        return finished && isBlocked().isDone();
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        ListenableFuture<?> blocked = pagePartitioner.isFull();
        return blocked.isDone() ? NOT_BLOCKED : blocked;
    }

    @Override
    public boolean needsInput()
    {
        return !finished && isBlocked().isDone();
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");

        if (page.getPositionCount() == 0) {
            return;
        }

        page = pagePreprocessor.apply(page);
        pagePartitioner.partitionPage(page);

        // TODO: PartitionedOutputOperator reports incorrect output data size #11770
        operatorContext.recordOutput(page.getSizeInBytes(), page.getPositionCount());

        systemMemoryContext.setBytes(pagePartitioner.getRetainedSizeInBytes());
    }

    @Override
    public Page getOutput()
    {
        return null;
    }

    private static class PartitionData
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(PartitionData.class).instanceSize();
        private static final int INITIAL_POSITION_COUNT = 64 * 1024;
        private static final int DEFAULT_ELEMENT_SIZE_IN_BYTES = 8;

        private final int partition;
        private final AtomicLong rowsAdded;
        private final AtomicLong pagesAdded;
        private final PagesSerde serde;
        private final Lifespan lifespan;
        private final int capacity;
        private final int channelCount;

        private int[] positions = new int[INITIAL_POSITION_COUNT];   // the default positions array for top level BlockEncodingBuffers
        private int[] rowSizes = new int[INITIAL_POSITION_COUNT];
        private int positionCount;  // number of positions to be copied for this partition
        private BlockEncodingBuffers[] blockEncodingBuffers;

        private int bufferedRowCount;
        private boolean bufferFull;

        PartitionData(int partition, int channelCount, int capacity, AtomicLong pagesAdded, AtomicLong rowsAdded, PagesSerde serde, Lifespan lifespan)
        {
            this.partition = partition;
            this.channelCount = channelCount;
            this.capacity = capacity;
            this.pagesAdded = requireNonNull(pagesAdded, "pagesAdded is null");
            this.rowsAdded = requireNonNull(rowsAdded, "rowsAdded is null");
            this.serde = requireNonNull(serde, "serde is null");
            this.lifespan = requireNonNull(lifespan, "lifespan is null");
        }

        public void resetPositions(int positionCount)
        {
            if (positions.length < positionCount) {
                positions = new int[max(2 * positions.length, positionCount)];
            }

            this.positionCount = 0;
        }

        public void appendPosition(int position)
        {
            positions[positionCount++] = position;
        }

        public void appendData(DecodedBlockNode[] decodedBlocks, int fixedWidthRowSize, List<Integer> variableWidthChannels, OutputBuffer outputBuffer)
        {
            checkArgument(decodedBlocks.length == channelCount, "Unexpected number of blocks");

            if (positionCount == 0) {
                return;
            }

            if (channelCount == 0) {
                bufferedRowCount += positionCount;
                return;
            }

            initializeBlockEncodingBuffers(decodedBlocks);

            for (int i = 0; i < channelCount; i++) {
                blockEncodingBuffers[i].setupDecodedBlocksAndPositions(decodedBlocks[i], positions, positionCount);
            }

            calculateRowSizes(fixedWidthRowSize, variableWidthChannels);

            int offset = 0;
            do {
                int batchSize = calculateNextBatchSize(fixedWidthRowSize, variableWidthChannels, offset);

                for (int i = 0; i < channelCount; i++) {
                    blockEncodingBuffers[i].setNextBatch(offset, batchSize);
                    blockEncodingBuffers[i].copyValues();
                }

                bufferedRowCount += batchSize;
                offset += batchSize;

                if (bufferFull) {
                    flush(outputBuffer);
                    bufferFull = false;
                }
            }
            while (offset < positionCount);
        }

        public long getInitialRetainedSizeInBytes()
        {
            return INSTANCE_SIZE + sizeOf(positions) + sizeOf(rowSizes);
        }

        public long getRetainedSizeInBytes()
        {
            long size = INSTANCE_SIZE + sizeOf(positions) + sizeOf(rowSizes);

            if (blockEncodingBuffers != null) {
                for (int i = 0; i < channelCount; i++) {
                    size += blockEncodingBuffers[i].getRetainedSizeInBytes();
                }
            }

            return size;
        }

        private void initializeBlockEncodingBuffers(DecodedBlockNode[] decodedBlocks)
        {
            // Create buffers has to be done after seeing the first page.
            if (blockEncodingBuffers == null) {
                blockEncodingBuffers = new BlockEncodingBuffers[channelCount];
                for (int i = 0; i < channelCount; i++) {
                    blockEncodingBuffers[i] = createBlockEncodingBuffers(decodedBlocks[i]);
                }
            }
        }

        private void calculateRowSizes(int fixedWidthRowSize, List<Integer> variableWidthChannels)
        {
            if (variableWidthChannels.isEmpty()) {
                return;
            }

            rowSizes = ensureCapacity(rowSizes, positionCount, true);

            for (int i : variableWidthChannels) {
                blockEncodingBuffers[i].accumulateRowSizes(rowSizes);
            }

            for (int i = 0; i < positionCount; i++) {
                rowSizes[i] += fixedWidthRowSize;
            }
        }

        private int calculateNextBatchSize(int fixedWidthRowSize, List<Integer> variableWidthChannels, int startPosition)
        {
            int bytesRemaining = capacity - getSerializedBuffersSizeInBytes();

            if (variableWidthChannels.isEmpty()) {
                int positionsFit = max(bytesRemaining / fixedWidthRowSize, 1);
                if (positionsFit <= positionCount - startPosition) {
                    bufferFull = true;
                    return positionsFit;
                }
                return positionCount - startPosition;
            }

            verify(rowSizes != null);
            for (int i = startPosition; i < positionCount; i++) {
                bytesRemaining -= rowSizes[i];

                if (bytesRemaining <= 0) {
                    bufferFull = true;
                    return max(i - startPosition, 1);
                }
            }

            return positionCount - startPosition;
        }

        private void flush(OutputBuffer outputBuffer)
        {
            if (bufferedRowCount == 0) {
                return;
            }

            SliceOutput output = new DynamicSliceOutput(toIntExact(getSerializedBuffersSizeInBytes()));
            output.writeInt(channelCount);

            for (int i = 0; i < channelCount; i++) {
                blockEncodingBuffers[i].serializeTo(output);
            }

            SerializedPage serializedPage = serde.serialize(output.slice(), bufferedRowCount);
            outputBuffer.enqueue(lifespan, partition, ImmutableList.of(serializedPage));
            pagesAdded.incrementAndGet();
            rowsAdded.addAndGet(bufferedRowCount);

            if (blockEncodingBuffers != null) {
                for (int i = 0; i < channelCount; i++) {
                    blockEncodingBuffers[i].resetBuffers();
                }
            }

            bufferedRowCount = 0;
        }

        private int getSerializedBuffersSizeInBytes()
        {
            int size = 0;

            for (int i = 0; i < channelCount; i++) {
                size += blockEncodingBuffers[i].getSerializedSizeInBytes();
            }

            return SIZE_OF_INT + size;  // channelCount takes one int
        }
    }

    private static class PagePartitioner
    {
        // flattener borrows one array for each nested Dictionary/Rle level. We assume the maximum nested level is 100.
        private static final int MAX_FLATTENER_OUTSTANDING_ARRAYS = 100;
        private static final int LONG_DECIMAL_SERIALIZED_BYTES = 17;
        private static final int SHORT_DECIMAL_SERIALIZED_BYTES = 9;
        private static final Map<Type, Integer> FIXED_WIDTH_TYPE_SERIALIZED_BYTES = ImmutableMap.<Type, Integer>builder()
                .put(BIGINT, 9)
                .put(INTEGER, 5)
                .put(BOOLEAN, 2)
                .put(DATE, 5)
                .put(DOUBLE, 9)
                .put(INTERVAL_DAY_TIME, 9)
                .put(INTERVAL_YEAR_MONTH, 5)
                .put(IPADDRESS, 17)
                .put(SMALLINT, 3)
                .put(TIME, 9)
                .put(TIME_WITH_TIME_ZONE, 9)
                .put(TIMESTAMP, 9)
                .put(TIMESTAMP_WITH_TIME_ZONE, 9)
                .put(TINYINT, 2)
                .put(UNKNOWN, 2)
                .build();

        private final OutputBuffer outputBuffer;
        private final PartitionFunction partitionFunction;
        private final List<Integer> partitionChannels;
        private final List<Optional<Block>> partitionConstants;
        private final PagesSerde serde;
        private final boolean replicatesAnyRow;
        private final OptionalInt nullChannel; // when present, send the position to every partition if this channel is null.
        private final AtomicLong rowsAdded = new AtomicLong();
        private final AtomicLong pagesAdded = new AtomicLong();

        private final BlockFlattener flattener = new BlockFlattener(new SimpleArrayAllocator(MAX_FLATTENER_OUTSTANDING_ARRAYS));

        private final PartitionData[] partitionData;
        private final List<Type> sourceTypes;
        private final List<Integer> variableWidthChannels;
        private final int fixedWidthRowSize;
        private final DecodedBlockNode[] decodedBlocks;

        private boolean hasAnyRowBeenReplicated;

        public PagePartitioner(
                PartitionFunction partitionFunction,
                List<Integer> partitionChannels,
                List<Optional<ConstantExpression>> partitionConstants,
                boolean replicatesAnyRow,
                OptionalInt nullChannel,
                OutputBuffer outputBuffer,
                PagesSerdeFactory serdeFactory,
                List<Type> sourceTypes,
                DataSize maxMemory,
                Lifespan lifespan)
        {
            this.partitionFunction = requireNonNull(partitionFunction, "pagePartitioner is null");
            this.partitionChannels = requireNonNull(partitionChannels, "partitionChannels is null");
            this.partitionConstants = requireNonNull(partitionConstants, "partitionConstants is null").stream()
                    .map(constant -> constant.map(ConstantExpression::getValueBlock))
                    .collect(toImmutableList());
            this.replicatesAnyRow = replicatesAnyRow;
            this.nullChannel = requireNonNull(nullChannel, "nullChannel is null");
            this.outputBuffer = requireNonNull(outputBuffer, "outputBuffer is null");
            this.serde = requireNonNull(serdeFactory, "serdeFactory is null").createPagesSerde();

            int partitionCount = partitionFunction.getPartitionCount();

            int pageSize = max(1, min(DEFAULT_MAX_PAGE_SIZE_IN_BYTES, toIntExact(maxMemory.toBytes()) / partitionCount));

            partitionData = new PartitionData[partitionCount];
            for (int i = 0; i < partitionCount; i++) {
                partitionData[i] = new PartitionData(i, sourceTypes.size(), pageSize, pagesAdded, rowsAdded, serde, lifespan);
            }

            this.sourceTypes = sourceTypes;
            decodedBlocks = new DecodedBlockNode[sourceTypes.size()];

            ImmutableList.Builder<Integer> variableWidthChannels = ImmutableList.builder();
            int fixedWidthRowSize = 0;
            for (int i = 0; i < sourceTypes.size(); i++) {
                int bytesPerPosition = getFixedWidthTypeSize(i);
                fixedWidthRowSize += bytesPerPosition;

                if (bytesPerPosition == 0) {
                    variableWidthChannels.add(i);
                }
            }
            this.variableWidthChannels = variableWidthChannels.build();
            this.fixedWidthRowSize = fixedWidthRowSize;
        }

        public ListenableFuture<?> isFull()
        {
            return outputBuffer.isFull();
        }

        public long getInitialRetainedSizeInBytes()
        {
            long size = 0;
            for (int i = 0; i < partitionData.length; i++) {
                size += partitionData[i].getInitialRetainedSizeInBytes();
            }

            return size;
        }

        public long getRetainedSizeInBytes()
        {
            long size = getInitialRetainedSizeInBytes();

            for (int i = 0; i < decodedBlocks.length; i++) {
                size += decodedBlocks[i].getRetainedSizeInBytes();
            }

            return size;
        }

        public PartitionedOutputInfo getInfo()
        {
            return new PartitionedOutputInfo(rowsAdded.get(), pagesAdded.get(), outputBuffer.getPeakMemoryUsage());
        }

        public void partitionPage(Page page)
        {
            // Populate positions to copy for each destination partition.
            int positionCount = page.getPositionCount();

            for (int i = 0; i < partitionData.length; i++) {
                partitionData[i].resetPositions(positionCount);
            }

            Block nullBlock = nullChannel.isPresent() ? page.getBlock(nullChannel.getAsInt()) : null;
            Page partitionFunctionArgs = getPartitionFunctionArguments(page);

            for (int position = 0; position < positionCount; position++) {
                boolean shouldReplicate = (replicatesAnyRow && !hasAnyRowBeenReplicated) ||
                        nullBlock != null && nullBlock.isNull(position);

                if (shouldReplicate) {
                    for (int i = 0; i < partitionData.length; i++) {
                        partitionData[i].appendPosition(position);
                    }
                    hasAnyRowBeenReplicated = true;
                }
                else {
                    int partition = partitionFunction.getPartition(partitionFunctionArgs, position);
                    partitionData[partition].appendPosition(position);
                }
            }

            // Decode the page just once. The decoded blocks will be fed to each PartitionData object to set up BlockEncodingBuffers.
            for (int i = 0; i < decodedBlocks.length; i++) {
                decodedBlocks[i] = decodeBlock(flattener, page.getBlock(i));
            }

            // Copy the data to their destination partitions and flush when the buffer is full.
            for (int i = 0; i < partitionData.length; i++) {
                partitionData[i].appendData(decodedBlocks, fixedWidthRowSize, variableWidthChannels, outputBuffer);
            }
        }

        public void flush()
        {
            for (int i = 0; i < partitionData.length; i++) {
                partitionData[i].flush(outputBuffer);
            }
        }

        private Page getPartitionFunctionArguments(Page page)
        {
            Block[] blocks = new Block[partitionChannels.size()];
            for (int i = 0; i < blocks.length; i++) {
                Optional<Block> partitionConstant = partitionConstants.get(i);
                if (partitionConstant.isPresent()) {
                    blocks[i] = new RunLengthEncodedBlock(partitionConstant.get(), page.getPositionCount());
                }
                else {
                    blocks[i] = page.getBlock(partitionChannels.get(i));
                }
            }
            return new Page(page.getPositionCount(), blocks);
        }

        private int getFixedWidthTypeSize(int channel)
        {
            Type type = sourceTypes.get(channel);
            int bytesPerPosition = FIXED_WIDTH_TYPE_SERIALIZED_BYTES.getOrDefault(type, 0);
            if (bytesPerPosition > 0) {
                return bytesPerPosition;
            }

            if (type instanceof DecimalType) {
                return ((DecimalType) type).isShort() ? SHORT_DECIMAL_SERIALIZED_BYTES : LONG_DECIMAL_SERIALIZED_BYTES;
            }
            return 0;
        }
    }

    @VisibleForTesting
    static DecodedBlockNode decodeBlock(BlockFlattener flattener, Block block)
    {
        try (BlockLease lease = flattener.flatten(block)) {
            Block decodedBlock = lease.get();

            if (decodedBlock instanceof ArrayBlock) {
                ColumnarArray columnarArray = ColumnarArray.toColumnarArray(decodedBlock);
                return new DecodedBlockNode(columnarArray, ImmutableList.of(decodeBlock(flattener, columnarArray.getElementsBlock())));
            }

            if (decodedBlock instanceof MapBlock) {
                ColumnarMap columnarMap = ColumnarMap.toColumnarMap(decodedBlock);
                return new DecodedBlockNode(columnarMap, ImmutableList.of(decodeBlock(flattener, columnarMap.getKeysBlock()), decodeBlock(flattener, columnarMap.getValuesBlock())));
            }

            if (decodedBlock instanceof RowBlock) {
                ColumnarRow columnarRow = ColumnarRow.toColumnarRow(decodedBlock);
                ImmutableList.Builder<DecodedBlockNode> children = ImmutableList.builder();
                for (int i = 0; i < columnarRow.getFieldCount(); i++) {
                    children.add(decodeBlock(flattener, columnarRow.getField(i)));
                }
                return new DecodedBlockNode(columnarRow, children.build());
            }

            if (decodedBlock instanceof DictionaryBlock) {
                return new DecodedBlockNode(decodedBlock, ImmutableList.of(decodeBlock(flattener, ((DictionaryBlock) decodedBlock).getDictionary())));
            }

            if (decodedBlock instanceof RunLengthEncodedBlock) {
                return new DecodedBlockNode(decodedBlock, ImmutableList.of(decodeBlock(flattener, ((RunLengthEncodedBlock) decodedBlock).getValue())));
            }

            return new DecodedBlockNode(decodedBlock, ImmutableList.of());
        }
    }
}
