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

import com.facebook.presto.block.BlockAssertions.Encoding;
import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockFlattener;
import com.facebook.presto.spi.block.DictionaryBlock;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.MapType;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SliceOutput;
import org.testng.annotations.Test;

import java.lang.invoke.MethodHandle;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.stream.IntStream;

import static com.facebook.presto.block.BlockAssertions.Encoding.DICTIONARY;
import static com.facebook.presto.block.BlockAssertions.Encoding.RUN_LENGTH;
import static com.facebook.presto.block.BlockAssertions.assertBlockEquals;
import static com.facebook.presto.block.BlockAssertions.createBooleansBlock;
import static com.facebook.presto.block.BlockAssertions.createDictionaryBlock;
import static com.facebook.presto.block.BlockAssertions.createIntsBlock;
import static com.facebook.presto.block.BlockAssertions.createLongDecimalsBlock;
import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createMapType;
import static com.facebook.presto.block.BlockAssertions.createNullBlock;
import static com.facebook.presto.block.BlockAssertions.createRLEBlock;
import static com.facebook.presto.block.BlockAssertions.createShortDecimalsBlock;
import static com.facebook.presto.block.BlockAssertions.createSmallintsBlock;
import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.block.BlockAssertions.wrapBlock;
import static com.facebook.presto.block.BlockSerdeUtil.readBlock;
import static com.facebook.presto.operator.BlockEncodingBuffers.createBlockEncodingBuffers;
import static com.facebook.presto.operator.OptimizedPartitionedOutputOperator.decodeBlock;
import static com.facebook.presto.spi.block.ArrayBlock.fromElementBlock;
import static com.facebook.presto.spi.block.MapBlock.fromKeyValueBlock;
import static com.facebook.presto.spi.block.MethodHandleUtil.compose;
import static com.facebook.presto.spi.block.MethodHandleUtil.nativeValueGetter;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DecimalType.createDecimalType;
import static com.facebook.presto.spi.type.Decimals.MAX_SHORT_PRECISION;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.TestingEnvironment.TYPE_MANAGER;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;

public class TestBlockEncodingBuffers
{
    private static final int POSITIONS_PER_BLOCK = 1000;

    private final Random random = new Random(0);

    @Test
    public void testBigint()
    {
        testBlock(BIGINT, createLongsBlock(POSITIONS_PER_BLOCK, true));
    }

    @Test
    public void testLongDecimal()
    {
        testBlock(createDecimalType(MAX_SHORT_PRECISION + 1), createLongDecimalsBlock(POSITIONS_PER_BLOCK, true));
    }

    @Test
    public void testInteger()
    {
        testBlock(INTEGER, createIntsBlock(POSITIONS_PER_BLOCK, true));
    }

    @Test
    public void testSmallint()
    {
        testBlock(SMALLINT, createSmallintsBlock(POSITIONS_PER_BLOCK, true));
    }

    @Test
    public void testBoolean()
    {
        testBlock(BOOLEAN, createBooleansBlock(POSITIONS_PER_BLOCK, true));
    }

    @Test
    public void testVarchar()
    {
        testBlock(VARCHAR, createStringsBlock(POSITIONS_PER_BLOCK, true, 10));
        testBlock(VARCHAR, createStringsBlock(POSITIONS_PER_BLOCK, true, 0));
        testBlock(VARCHAR, createRLEBlock(createStringsBlock(POSITIONS_PER_BLOCK, true, 0), POSITIONS_PER_BLOCK));
    }

    @Test
    public void testArray()
    {
        testNestedBlock(new ArrayType(BIGINT));
        testNestedBlock(new ArrayType(createDecimalType(MAX_SHORT_PRECISION)));
        testNestedBlock(new ArrayType(createDecimalType(MAX_SHORT_PRECISION + 1)));
        testNestedBlock(new ArrayType(INTEGER));
        testNestedBlock(new ArrayType(SMALLINT));
        testNestedBlock(new ArrayType(BOOLEAN));
        testNestedBlock(new ArrayType(VARCHAR));

        testNestedBlock(new ArrayType(new ArrayType(BIGINT)));
        testNestedBlock(new ArrayType(new ArrayType(createDecimalType(MAX_SHORT_PRECISION))));
        testNestedBlock(new ArrayType(new ArrayType(createDecimalType(MAX_SHORT_PRECISION + 1))));
        testNestedBlock(new ArrayType(new ArrayType(INTEGER)));
        testNestedBlock(new ArrayType(new ArrayType(SMALLINT)));
        testNestedBlock(new ArrayType(new ArrayType(BOOLEAN)));
        testNestedBlock(new ArrayType(new ArrayType(VARCHAR)));

        testNestedBlock(new ArrayType(new ArrayType(new ArrayType(BIGINT))));
        testNestedBlock(new ArrayType(new ArrayType(new ArrayType(createDecimalType(MAX_SHORT_PRECISION)))));
        testNestedBlock(new ArrayType(new ArrayType(new ArrayType(createDecimalType(MAX_SHORT_PRECISION + 1)))));
        testNestedBlock(new ArrayType(new ArrayType(new ArrayType(INTEGER))));
        testNestedBlock(new ArrayType(new ArrayType(new ArrayType(SMALLINT))));
        testNestedBlock(new ArrayType(new ArrayType(new ArrayType(BOOLEAN))));
        testNestedBlock(new ArrayType(new ArrayType(new ArrayType(VARCHAR))));
    }

    @Test
    public void testMap()
    {
        testNestedBlock(createMapType(BIGINT, BIGINT));
        testNestedBlock(createMapType(createDecimalType(MAX_SHORT_PRECISION + 1), createDecimalType(MAX_SHORT_PRECISION + 1)));
        testNestedBlock(createMapType(INTEGER, INTEGER));
        testNestedBlock(createMapType(SMALLINT, SMALLINT));
        testNestedBlock(createMapType(BOOLEAN, BOOLEAN));
        testNestedBlock(createMapType(VARCHAR, VARCHAR));

        testNestedBlock(createMapType(BIGINT, createMapType(BIGINT, BIGINT)));
        testNestedBlock(createMapType(createDecimalType(MAX_SHORT_PRECISION + 1), createMapType(createDecimalType(MAX_SHORT_PRECISION + 1), createDecimalType(MAX_SHORT_PRECISION + 1))));
        testNestedBlock(createMapType(INTEGER, createMapType(INTEGER, INTEGER)));
        testNestedBlock(createMapType(SMALLINT, createMapType(SMALLINT, SMALLINT)));
        testNestedBlock(createMapType(BOOLEAN, createMapType(BOOLEAN, BOOLEAN)));
        testNestedBlock(createMapType(VARCHAR, createMapType(VARCHAR, VARCHAR)));

        testNestedBlock(createMapType(createMapType(createMapType(BIGINT, BIGINT), BIGINT), createMapType(BIGINT, BIGINT)));
        testNestedBlock(createMapType(
                createMapType(createMapType(createDecimalType(MAX_SHORT_PRECISION + 1), createDecimalType(MAX_SHORT_PRECISION + 1)), createDecimalType(MAX_SHORT_PRECISION + 1)),
                createMapType(createDecimalType(MAX_SHORT_PRECISION + 1), createDecimalType(MAX_SHORT_PRECISION + 1))));
        testNestedBlock(createMapType(createMapType(createMapType(INTEGER, INTEGER), INTEGER), createMapType(INTEGER, INTEGER)));
        testNestedBlock(createMapType(createMapType(createMapType(SMALLINT, SMALLINT), SMALLINT), createMapType(SMALLINT, SMALLINT)));
        testNestedBlock(createMapType(createMapType(createMapType(BOOLEAN, BOOLEAN), BOOLEAN), createMapType(BOOLEAN, BOOLEAN)));
        testNestedBlock(createMapType(createMapType(createMapType(VARCHAR, VARCHAR), VARCHAR), createMapType(VARCHAR, VARCHAR)));

        testNestedBlock(createMapType(BIGINT, new ArrayType(BIGINT)));
        testNestedBlock(createMapType(createDecimalType(MAX_SHORT_PRECISION + 1), new ArrayType(createDecimalType(MAX_SHORT_PRECISION + 1))));
        testNestedBlock(createMapType(INTEGER, new ArrayType(INTEGER)));
        testNestedBlock(createMapType(SMALLINT, new ArrayType(SMALLINT)));
        testNestedBlock(createMapType(BOOLEAN, new ArrayType(BOOLEAN)));
        testNestedBlock(createMapType(VARCHAR, new ArrayType(VARCHAR)));
    }

    private void testBlock(Type type, Block block)
    {
        requireNonNull(type);

        assertSerialized(type, createNullBlock(type, POSITIONS_PER_BLOCK));
        assertSerialized(type, block);

        assertSerialized(type, wrapBlock(block, POSITIONS_PER_BLOCK, ImmutableList.of(DICTIONARY)));
        assertSerialized(type, wrapBlock(block, POSITIONS_PER_BLOCK, ImmutableList.of(RUN_LENGTH)));
        assertSerialized(type, wrapBlock(block, POSITIONS_PER_BLOCK, ImmutableList.of(DICTIONARY, DICTIONARY, RUN_LENGTH)));
        assertSerialized(type, wrapBlock(block, POSITIONS_PER_BLOCK, ImmutableList.of(RUN_LENGTH, DICTIONARY, DICTIONARY)));
        assertSerialized(type, wrapBlock(block, POSITIONS_PER_BLOCK, ImmutableList.of(DICTIONARY, RUN_LENGTH, DICTIONARY, RUN_LENGTH)));
        assertSerialized(type, wrapBlock(block, POSITIONS_PER_BLOCK, ImmutableList.of(RUN_LENGTH, DICTIONARY, RUN_LENGTH, DICTIONARY)));

        assertSerialized(type, block.getRegion(POSITIONS_PER_BLOCK / 2, POSITIONS_PER_BLOCK / 3));

        assertSerialized(type, wrapBlock(block, POSITIONS_PER_BLOCK, ImmutableList.of(DICTIONARY)).getRegion(POSITIONS_PER_BLOCK / 2, POSITIONS_PER_BLOCK / 3));
        assertSerialized(type, wrapBlock(block, POSITIONS_PER_BLOCK, ImmutableList.of(RUN_LENGTH)).getRegion(POSITIONS_PER_BLOCK / 2, POSITIONS_PER_BLOCK / 3));
        assertSerialized(type, wrapBlock(block, POSITIONS_PER_BLOCK, ImmutableList.of(DICTIONARY, DICTIONARY, RUN_LENGTH)).getRegion(POSITIONS_PER_BLOCK / 2, POSITIONS_PER_BLOCK / 3));
        assertSerialized(type, wrapBlock(block, POSITIONS_PER_BLOCK, ImmutableList.of(RUN_LENGTH, DICTIONARY, DICTIONARY)).getRegion(POSITIONS_PER_BLOCK / 2, POSITIONS_PER_BLOCK / 3));
        assertSerialized(type, wrapBlock(block, POSITIONS_PER_BLOCK, ImmutableList.of(DICTIONARY, RUN_LENGTH, DICTIONARY, RUN_LENGTH)).getRegion(POSITIONS_PER_BLOCK / 2, POSITIONS_PER_BLOCK / 3));
        assertSerialized(type, wrapBlock(block, POSITIONS_PER_BLOCK, ImmutableList.of(RUN_LENGTH, DICTIONARY, RUN_LENGTH, DICTIONARY)).getRegion(POSITIONS_PER_BLOCK / 2, POSITIONS_PER_BLOCK / 3));

        assertSerialized(type, wrapBlock(block.getRegion(POSITIONS_PER_BLOCK / 2, POSITIONS_PER_BLOCK / 3), POSITIONS_PER_BLOCK, ImmutableList.of(DICTIONARY)));
        assertSerialized(type, wrapBlock(block.getRegion(POSITIONS_PER_BLOCK / 2, POSITIONS_PER_BLOCK / 3), POSITIONS_PER_BLOCK, ImmutableList.of(RUN_LENGTH)));
        assertSerialized(type, wrapBlock(block.getRegion(POSITIONS_PER_BLOCK / 2, POSITIONS_PER_BLOCK / 3), POSITIONS_PER_BLOCK, ImmutableList.of(DICTIONARY, DICTIONARY, RUN_LENGTH)));
        assertSerialized(type, wrapBlock(block.getRegion(POSITIONS_PER_BLOCK / 2, POSITIONS_PER_BLOCK / 3), POSITIONS_PER_BLOCK, ImmutableList.of(RUN_LENGTH, DICTIONARY, DICTIONARY)));
        assertSerialized(type, wrapBlock(block.getRegion(POSITIONS_PER_BLOCK / 2, POSITIONS_PER_BLOCK / 3), POSITIONS_PER_BLOCK, ImmutableList.of(DICTIONARY, RUN_LENGTH, DICTIONARY, RUN_LENGTH)));
        assertSerialized(type, wrapBlock(block.getRegion(POSITIONS_PER_BLOCK / 2, POSITIONS_PER_BLOCK / 3), POSITIONS_PER_BLOCK, ImmutableList.of(RUN_LENGTH, DICTIONARY, RUN_LENGTH, DICTIONARY)));
    }

    private void testNestedBlock(Type type)
    {
        requireNonNull(type);

        assertSerialized(type, createNullBlock(type, POSITIONS_PER_BLOCK));

        BlockStatus blockStatus = buildBlockStatusWithType(type, POSITIONS_PER_BLOCK, false, ImmutableList.of());
        assertSerialized(type, blockStatus.block, blockStatus.expectedRowSizes);

        blockStatus = buildBlockStatusWithType(type, POSITIONS_PER_BLOCK, false, ImmutableList.of(DICTIONARY));
        assertSerialized(type, blockStatus.block, blockStatus.expectedRowSizes);

        blockStatus = buildBlockStatusWithType(type, POSITIONS_PER_BLOCK, false, ImmutableList.of(RUN_LENGTH));
        assertSerialized(type, blockStatus.block, blockStatus.expectedRowSizes);

        blockStatus = buildBlockStatusWithType(type, POSITIONS_PER_BLOCK, false, ImmutableList.of(DICTIONARY, DICTIONARY, RUN_LENGTH));
        assertSerialized(type, blockStatus.block, blockStatus.expectedRowSizes);

        blockStatus = buildBlockStatusWithType(type, POSITIONS_PER_BLOCK, false, ImmutableList.of(RUN_LENGTH, DICTIONARY, DICTIONARY));
        assertSerialized(type, blockStatus.block, blockStatus.expectedRowSizes);

        blockStatus = buildBlockStatusWithType(type, POSITIONS_PER_BLOCK, false, ImmutableList.of(DICTIONARY, RUN_LENGTH, DICTIONARY, RUN_LENGTH));
        assertSerialized(type, blockStatus.block, blockStatus.expectedRowSizes);

        blockStatus = buildBlockStatusWithType(type, POSITIONS_PER_BLOCK, false, ImmutableList.of(RUN_LENGTH, DICTIONARY, RUN_LENGTH, DICTIONARY));
        assertSerialized(type, blockStatus.block, blockStatus.expectedRowSizes);

        blockStatus = buildBlockStatusWithType(type, POSITIONS_PER_BLOCK, true, ImmutableList.of());
        assertSerialized(type, blockStatus.block, blockStatus.expectedRowSizes);

        blockStatus = buildBlockStatusWithType(type, POSITIONS_PER_BLOCK, true, ImmutableList.of(DICTIONARY));
        assertSerialized(type, blockStatus.block, blockStatus.expectedRowSizes);

        blockStatus = buildBlockStatusWithType(type, POSITIONS_PER_BLOCK, true, ImmutableList.of(RUN_LENGTH));
        assertSerialized(type, blockStatus.block, blockStatus.expectedRowSizes);

        blockStatus = buildBlockStatusWithType(type, POSITIONS_PER_BLOCK, true, ImmutableList.of(DICTIONARY, DICTIONARY, RUN_LENGTH));
        assertSerialized(type, blockStatus.block, blockStatus.expectedRowSizes);

        blockStatus = buildBlockStatusWithType(type, POSITIONS_PER_BLOCK, true, ImmutableList.of(RUN_LENGTH, DICTIONARY, DICTIONARY));
        assertSerialized(type, blockStatus.block, blockStatus.expectedRowSizes);

        blockStatus = buildBlockStatusWithType(type, POSITIONS_PER_BLOCK, true, ImmutableList.of(DICTIONARY, RUN_LENGTH, DICTIONARY, RUN_LENGTH));
        assertSerialized(type, blockStatus.block, blockStatus.expectedRowSizes);

        blockStatus = buildBlockStatusWithType(type, POSITIONS_PER_BLOCK, true, ImmutableList.of(RUN_LENGTH, DICTIONARY, RUN_LENGTH, DICTIONARY));
        assertSerialized(type, blockStatus.block, blockStatus.expectedRowSizes);
    }

    private static void assertSerialized(Type type, Block block)
    {
        assertSerialized(type, block, null);
    }

    private static void assertSerialized(Type type, Block block, int[] expectedRowSizes)
    {
        // The number 5 was chosen to be greater than the maximum Dictionary/RLE nested level
        BlockFlattener flattener = new BlockFlattener(new SimpleArrayAllocator(5));
        DecodedBlockNode decodedBlock = decodeBlock(flattener, block);

        BlockEncodingBuffers buffers = createBlockEncodingBuffers(decodedBlock);

        int[] positions = IntStream.range(0, block.getPositionCount() / 2).toArray();
        copyPositions(decodedBlock, buffers, positions, expectedRowSizes);

        positions = IntStream.range(block.getPositionCount() / 2, block.getPositionCount()).toArray();
        copyPositions(decodedBlock, buffers, positions, expectedRowSizes);

        assertBlockEquals(type, serialize(buffers), block);

        buffers.resetBuffers();

        positions = IntStream.range(0, block.getPositionCount()).filter(n -> n % 2 == 0).toArray();
        Block expectedBlock = block.copyPositions(positions, 0, positions.length);
        copyPositions(decodedBlock, buffers, positions, expectedRowSizes);

        assertBlockEquals(type, serialize(buffers), expectedBlock);
    }

    private static void copyPositions(DecodedBlockNode decodedBlock, BlockEncodingBuffers buffers, int[] positions, int[] expectedRowSizes)
    {
        buffers.setupDecodedBlocksAndPositions(decodedBlock, positions, positions.length);

        if (expectedRowSizes != null) {
            int[] actualSizes = new int[positions.length];
            buffers.accumulateRowSizes(actualSizes);

            int[] expectedSizes = Arrays.stream(positions).map(i -> expectedRowSizes[i]).toArray();
            assertEquals(actualSizes, expectedSizes);
        }

        buffers.setNextBatch(0, positions.length);
        buffers.copyValues();
    }

    private static Block serialize(BlockEncodingBuffers buffers)
    {
        SliceOutput output = new DynamicSliceOutput(toIntExact(buffers.getSerializedSizeInBytes()));
        buffers.serializeTo(output);

        BlockEncodingManager blockEncodingSerde = new BlockEncodingManager(TYPE_MANAGER);
        return readBlock(blockEncodingSerde, output.slice().getInput());
    }

    private BlockStatus buildBlockStatusWithType(Type type, int positionCount, boolean isView, List<Encoding> wrappings)
    {
        return buildBlockStatusWithType(type, positionCount, isView, true, wrappings);
    }

    private BlockStatus buildBlockStatusWithType(Type type, int positionCount, boolean isView, boolean allowNulls, List<Encoding> wrappings)
    {
        BlockStatus blockStatus = null;

        if (isView) {
            positionCount *= 2;
        }

        if (type == BIGINT) {
            blockStatus = buildBigintBlockStatus(positionCount, allowNulls);
        }
        else if (type instanceof DecimalType) {
            if (!((DecimalType) type).isShort()) {
                blockStatus = buildLongDecimalBlockStatus(positionCount, allowNulls);
            }
            else {
                blockStatus = buildShortDecimalBlockStatus(positionCount, allowNulls);
            }
        }
        else if (type == INTEGER) {
            blockStatus = buildIntegerBlockStatus(positionCount, allowNulls);
        }
        else if (type == SMALLINT) {
            blockStatus = buildSmallintBlockStatus(positionCount, allowNulls);
        }
        else if (type == BOOLEAN) {
            blockStatus = buildBooleanBlockStatus(positionCount, allowNulls);
        }
        else if (type == VARCHAR) {
            blockStatus = buildVarcharBlockStatus(positionCount, allowNulls, 10);
        }
        else {
            // Nested types
            // Build isNull and offsets of size positionCount
            boolean[] isNull = new boolean[positionCount];
            int[] offsets = new int[positionCount + 1];
            for (int position = 0; position < positionCount; position++) {
                if (allowNulls && position % 7 == 0) {
                    isNull[position] = true;
                    offsets[position + 1] = offsets[position];
                }
                else {
                    offsets[position + 1] = offsets[position] + random.nextInt(10);
                }
            }

            // Build the nested block of size offsets[positionCount].
            if (type instanceof ArrayType) {
                blockStatus = buildArrayBlockStatus((ArrayType) type, positionCount, isView, isNull, offsets, allowNulls, wrappings);
            }
            else if (type instanceof MapType) {
                blockStatus = buildMapBlockStatus((MapType) type, positionCount, isView, isNull, offsets, allowNulls, wrappings);
            }
            else {
                throw new UnsupportedOperationException(format("type %s is not supported.", type));
            }
        }

        if (isView) {
            positionCount /= 2;
            int offset = positionCount / 2;
            Block blockView = blockStatus.block.getRegion(offset, positionCount);
            int[] expectedRowSizesView = Arrays.stream(blockStatus.expectedRowSizes, offset, offset + positionCount).toArray();
            blockStatus = new BlockStatus(blockView, expectedRowSizesView);
        }

        blockStatus = buildDictRleBlockStatus(blockStatus, positionCount, wrappings);

        return blockStatus;
    }

    private static BlockStatus buildBigintBlockStatus(int positionCount, boolean allowNulls)
    {
        Block block = createLongsBlock(positionCount, allowNulls);
        int[] expectedRowSizes = IntStream.generate(() -> LongArrayBlockEncodingBuffers.POSITION_SIZE).limit(positionCount).toArray();

        return new BlockStatus(block, expectedRowSizes);
    }

    private static BlockStatus buildShortDecimalBlockStatus(int positionCount, boolean allowNulls)
    {
        Block block = createShortDecimalsBlock(positionCount, allowNulls);
        int[] expectedRowSizes = IntStream.generate(() -> LongArrayBlockEncodingBuffers.POSITION_SIZE).limit(positionCount).toArray();

        return new BlockStatus(block, expectedRowSizes);
    }

    private static BlockStatus buildLongDecimalBlockStatus(int positionCount, boolean allowNulls)
    {
        Block block = createLongDecimalsBlock(positionCount, allowNulls);
        int[] expectedRowSizes = IntStream.generate(() -> Int128ArrayBlockEncodingBuffers.POSITION_SIZE).limit(positionCount).toArray();

        return new BlockStatus(block, expectedRowSizes);
    }

    private BlockStatus buildIntegerBlockStatus(int positionCount, boolean allowNulls)
    {
        Block block = createIntsBlock(positionCount, allowNulls);
        int[] expectedRowSizes = IntStream.generate(() -> IntArrayBlockEncodingBuffers.POSITION_SIZE).limit(positionCount).toArray();

        return new BlockStatus(block, expectedRowSizes);
    }

    private BlockStatus buildSmallintBlockStatus(int positionCount, boolean allowNulls)
    {
        Block block = createSmallintsBlock(positionCount, allowNulls);
        int[] expectedRowSizes = IntStream.generate(() -> ShortArrayBlockEncodingBuffers.POSITION_SIZE).limit(positionCount).toArray();

        return new BlockStatus(block, expectedRowSizes);
    }

    private BlockStatus buildBooleanBlockStatus(int positionCount, boolean allowNulls)
    {
        Block block = createBooleansBlock(positionCount, allowNulls);
        int[] expectedRowSizes = IntStream.generate(() -> ByteArrayBlockEncodingBuffers.POSITION_SIZE).limit(positionCount).toArray();

        return new BlockStatus(block, expectedRowSizes);
    }

    private BlockStatus buildVarcharBlockStatus(int positionCount, boolean allowNulls, int maxStringLength)
    {
        Block block = createStringsBlock(positionCount, allowNulls, maxStringLength);

        int[] expectedRowSizes = IntStream
                .range(0, positionCount)
                .map(i -> block.getSliceLength(i) + VariableWidthBlockEncodingBuffers.POSITION_SIZE)
                .toArray();

        return new BlockStatus(block, expectedRowSizes);
    }

    private BlockStatus buildDictRleBlockStatus(BlockStatus blockStatus, int positionCount, List<Encoding> wrappings)
    {
        checkArgument(blockStatus.block.getPositionCount() == positionCount);

        if (wrappings.isEmpty()) {
            return blockStatus;
        }

        BlockStatus wrappedBlockStatus = blockStatus;
        for (int i = wrappings.size() - 1; i >= 0; i--) {
            switch (wrappings.get(i)) {
                case DICTIONARY:
                    wrappedBlockStatus = buildDictionaryBlockStatus(blockStatus, positionCount);
                    break;
                case RUN_LENGTH:
                    wrappedBlockStatus = buildRleBlockStatus(blockStatus, positionCount);
                    break;
                default:
                    throw new IllegalArgumentException(format("wrappings %s is incorrect", wrappings));
            }
        }
        return wrappedBlockStatus;
    }

    private BlockStatus buildDictionaryBlockStatus(BlockStatus dictionary, int positionCount)
    {
        DictionaryBlock dictionaryBlock = createDictionaryBlock(dictionary.block, positionCount);
        int[] mappedExpectedRowSizes = IntStream.range(0, positionCount).map(i -> dictionary.expectedRowSizes[dictionaryBlock.getId(i)]).toArray();
        return new BlockStatus(dictionaryBlock, mappedExpectedRowSizes);
    }

    private BlockStatus buildRleBlockStatus(BlockStatus blockStatus, int positionCount)
    {
        int[] expectedRowSizes = new int[positionCount];
        // When we contructed the Rle block, we chose the row at the middle.
        Arrays.setAll(expectedRowSizes, i -> blockStatus.expectedRowSizes[blockStatus.block.getPositionCount() / 2]);
        return new BlockStatus(
                createRLEBlock(blockStatus.block, positionCount),
                expectedRowSizes);
    }

    private BlockStatus buildArrayBlockStatus(
            ArrayType arrayType,
            int positionCount,
            boolean isView,
            boolean[] isNull,
            int[] offsets,
            boolean allowNulls,
            List<Encoding> wrappings)
    {
        requireNonNull(isNull);
        requireNonNull(offsets);

        BlockStatus blockStatus;

        BlockStatus valuesBlockStatus = buildBlockStatusWithType(
                arrayType.getElementType(),
                offsets[positionCount],
                isView,
                allowNulls,
                wrappings);

        int[] expectedRowSizes = IntStream.range(0, positionCount)
                .map(i -> ArrayBlockEncodingBuffers.POSITION_SIZE + Arrays.stream(valuesBlockStatus.expectedRowSizes, offsets[i], offsets[i + 1]).sum())
                .toArray();

        blockStatus = new BlockStatus(
                fromElementBlock(positionCount, Optional.of(isNull), offsets, valuesBlockStatus.block),
                expectedRowSizes);
        return blockStatus;
    }

    private BlockStatus buildMapBlockStatus(
            MapType mapType,
            int positionCount,
            boolean isView,
            boolean[] isNull,
            int[] offsets,
            boolean allowNulls,
            List<Encoding> wrappings)
    {
        requireNonNull(isNull);

        BlockStatus blockStatus;

        BlockStatus keyBlockStatus = buildBlockStatusWithType(
                mapType.getKeyType(),
                offsets[positionCount],
                isView,
                false,
                wrappings);
        BlockStatus valueBlockStatus = buildBlockStatusWithType(
                mapType.getValueType(),
                offsets[positionCount],
                isView,
                allowNulls,
                wrappings);

        int[] expectedKeySizes = keyBlockStatus.expectedRowSizes;
        int[] expectedValueSizes = valueBlockStatus.expectedRowSizes;
        // Use expectedKeySizes for the total size for both key and values
        Arrays.setAll(expectedKeySizes, i -> expectedKeySizes[i] + expectedValueSizes[i]);
        int[] expectedRowSizes = IntStream.range(0, positionCount)
                .map(i -> MapBlockEncodingBuffers.POSITION_SIZE + Arrays.stream(expectedKeySizes, offsets[i], offsets[i + 1]).sum())
                .toArray();

        Type keyType = mapType.getKeyType();
        MethodHandle keyNativeEquals = TYPE_MANAGER.resolveOperator(OperatorType.EQUAL, ImmutableList.of(keyType, keyType));
        MethodHandle keyBlockNativeEquals = compose(keyNativeEquals, nativeValueGetter(keyType));
        MethodHandle keyNativeHashCode = TYPE_MANAGER.resolveOperator(OperatorType.HASH_CODE, ImmutableList.of(keyType));
        MethodHandle keyBlockHashCode = compose(keyNativeHashCode, nativeValueGetter(keyType));

        blockStatus = new BlockStatus(
                fromKeyValueBlock(
                        Optional.of(isNull),
                        offsets,
                        keyBlockStatus.block,
                        valueBlockStatus.block,
                        mapType,
                        keyBlockNativeEquals,
                        keyNativeHashCode,
                        keyBlockHashCode),
                expectedRowSizes);
        return blockStatus;
    }

    private static class BlockStatus
    {
        private final Block block;
        private final int[] expectedRowSizes;

        BlockStatus(Block block, int[] expectedRowSizes)
        {
            this.block = requireNonNull(block, "block is null");
            this.expectedRowSizes = requireNonNull(expectedRowSizes, "expectedRowSizes is null");
        }
    }
}
