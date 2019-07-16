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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.DictionaryBlock;
import com.facebook.presto.spi.block.Int128ArrayBlock;
import com.facebook.presto.spi.block.IntArrayBlock;
import com.facebook.presto.spi.block.LongArrayBlock;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import io.airlift.slice.SliceOutput;

import javax.annotation.Nullable;

import java.util.Arrays;

import static com.facebook.presto.array.Arrays.ensureCapacity;
import static com.facebook.presto.operator.UncheckedByteArrays.setByteUnchecked;
import static com.facebook.presto.spi.MoreByteArrays.fill;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public abstract class BlockEncodingBuffers
{
    private static final int BITS_IN_BYTE = 8;

    // The block after peeling off the Dictionary or RLE wrappings.
    protected Block decodedBlock;

    // The number of positions (rows) to be copied from the incoming block.
    protected int positionCount;

    // The number of positions (rows) to be copied from the incoming block within current batch.
    protected int batchSize;

    // The offset into positions/mappedPositions array that the current batch starts copying.
    // I.e. in each batch we copy the values of rows from positions[positionsOffset] to positions[positionsOffset + batchSize]
    protected int positionsOffset;

    // The number of positions (rows) buffered so far
    protected int bufferedPositionCount;

    // Whether the positions array was mapped to mappedPositions
    protected boolean positionsMapped;

    // The positions (row numbers) of the incoming Block to be copied to this buffer.
    // All top level BlockEncodingBuffers for the same partition share the same positions array.
    private int[] positions;

    // The mapped positions of the incoming Dictionary or RLE Block
    @Nullable
    private int[] mappedPositions;

    @Nullable
    private byte[] nullsBuffer;

    // The address offset in the nullsBuffer if new values are to be added.
    private int nullsBufferIndex;

    // For each batch we put the nulls values up to a multiple of 8 into nullsBuffer. The rest is kept in remainingNulls.
    private final boolean[] remainingNulls = new boolean[BITS_IN_BYTE];

    // Number of null values not put into nullsBuffer from last batch
    private int remainingNullsCount;

    // Boolean indicating whether there are any null values in the nullsBuffer. It is possible that all values are non-null.
    private boolean hasEncodedNulls;

    public static BlockEncodingBuffers createBlockEncodingBuffers(DecodedBlockNode decodedBlockNode)
    {
        requireNonNull(decodedBlockNode, "decodedBlockNode is null");

        Object decodedBlock = decodedBlockNode.getDecodedBlock();

        // Skip the Dictionary/Rle block node. The mapping info is not needed when creating buffers.
        if (decodedBlock instanceof DictionaryBlock) {
            decodedBlockNode = decodedBlockNode.getChildren().get(0);
            decodedBlock = decodedBlockNode.getDecodedBlock();
        }
        else if (decodedBlock instanceof RunLengthEncodedBlock) {
            decodedBlockNode = decodedBlockNode.getChildren().get(0);
            decodedBlock = decodedBlockNode.getDecodedBlock();
        }

        verify(!(decodedBlock instanceof DictionaryBlock), "Nested RLEs and dictionaries are not supported");
        verify(!(decodedBlock instanceof RunLengthEncodedBlock), "Nested RLEs and dictionaries are not supported");

        if (decodedBlock instanceof LongArrayBlock) {
            return new LongArrayBlockEncodingBuffers();
        }

        if (decodedBlock instanceof Int128ArrayBlock) {
            return new Int128ArrayBlockEncodingBuffers();
        }

        if (decodedBlock instanceof IntArrayBlock) {
            return new IntArrayBlockEncodingBuffers();
        }

        throw new IllegalArgumentException("Unsupported encoding: " + decodedBlock.getClass().getSimpleName());
    }

    public abstract void resetBuffers();

    public void setNextBatch(int positionsOffset, int batchSize)
    {
        this.positionsOffset = positionsOffset;
        this.batchSize = batchSize;
    }

    public abstract void copyValues();

    public abstract void serializeTo(SliceOutput output);

    public abstract long getRetainedSizeInBytes();

    public abstract long getSerializedSizeInBytes();

    public abstract void accumulateRowSizes(int[] rowSizes);

    // positionOffsets: the indexes to the positions in current buffer, which point to the boundaries of top level rows.
    // positionCount: top level position count.
    protected abstract void accumulateRowSizes(int[] positionOffsets, int positionCount, int[] rowSizes);

    public void setupDecodedBlocksAndPositions(DecodedBlockNode decodedBlockNode, int[] positions, int positionCount)
    {
        requireNonNull(decodedBlockNode, "decodedBlockNode is null");
        requireNonNull(positions, "positions is null");

        this.positions = positions;
        this.positionCount = positionCount;
        this.positionsOffset = 0;

        setupDecodedBlockAndMapPositions(decodedBlockNode);
    }

    protected void setupDecodedBlockAndMapPositions(DecodedBlockNode decodedBlockNode)
    {
        requireNonNull(decodedBlockNode, "decodedBlockNode is null");
        decodedBlock = (Block) mapPositions(decodedBlockNode).getDecodedBlock();
    }

    protected void resetNullsBuffer()
    {
        nullsBufferIndex = 0;
        remainingNullsCount = 0;
        hasEncodedNulls = false;
    }

    protected DecodedBlockNode mapPositions(DecodedBlockNode decodedBlockNode)
    {
        Object decodedObject = decodedBlockNode.getDecodedBlock();

        if (decodedObject instanceof DictionaryBlock) {
            DictionaryBlock dictionaryBlock = (DictionaryBlock) decodedObject;
            mappedPositions = ensureCapacity(mappedPositions, positionCount);

            for (int i = 0; i < positionCount; i++) {
                mappedPositions[i] = dictionaryBlock.getId(positions[i]);
            }
            positionsMapped = true;
            return decodedBlockNode.getChildren().get(0);
        }

        if (decodedObject instanceof RunLengthEncodedBlock) {
            mappedPositions = ensureCapacity(mappedPositions, positionCount, true);
            positionsMapped = true;
            return decodedBlockNode.getChildren().get(0);
        }

        positionsMapped = false;
        return decodedBlockNode;
    }

    protected int[] getPositions()
    {
        if (positionsMapped) {
            verify(mappedPositions != null);
            return mappedPositions;
        }

        return positions;
    }

    protected void appendNulls()
    {
        if (decodedBlock.mayHaveNull()) {
            // Write to nullsBuffer if there is a possibility to have nulls. It is possible that the
            // decodedBlock contains nulls, but rows that go into this partition don't. Write to nullsBuffer anyway.
            nullsBuffer = ensureCapacity(nullsBuffer, nullsBufferIndex + batchSize / BITS_IN_BYTE + 1, 2.0f, true);

            int bufferedNullsCount = nullsBufferIndex * BITS_IN_BYTE + remainingNullsCount;
            if (bufferedPositionCount > bufferedNullsCount) {
                // There are no nulls in positions (bufferedNullsCount, bufferedPositionCount]
                encodeNonNullsAsBits(bufferedPositionCount - bufferedNullsCount);
            }

            // Append this batch
            encodeNullsAsBits(decodedBlock);
        }
        else if (containsNull()) {
            // There were nulls in previously buffered rows, but for this batch there can't be any nulls.
            // Any how we need to append 0's for this batch.
            nullsBuffer = ensureCapacity(nullsBuffer, nullsBufferIndex + batchSize / BITS_IN_BYTE + 1, 2.0f, true);
            encodeNonNullsAsBits(batchSize);
        }
    }

    protected static void writeLengthPrefixedString(SliceOutput output, String value)
    {
        byte[] bytes = value.getBytes(UTF_8);
        output.writeInt(bytes.length);
        output.writeBytes(bytes);
    }

    protected void serializeNullsTo(SliceOutput output)
    {
        encodeRemainingNullsAsBits();

        if (hasEncodedNulls) {
            output.writeBoolean(true);
            output.appendBytes(nullsBuffer, 0, nullsBufferIndex);
        }
        else {
            output.writeBoolean(false);
        }
    }

    private boolean containsNull()
    {
        if (hasEncodedNulls) {
            return true;
        }

        for (int i = 0; i < remainingNullsCount; i++) {
            if (remainingNulls[i]) {
                return true;
            }
        }
        return false;
    }

    private void encodeNullsAsBits(Block block)
    {
        int[] positions = getPositions();

        if (remainingNullsCount + batchSize < BITS_IN_BYTE) {
            // just put all of this batch to remainingNulls
            for (int i = 0; i < batchSize; i++) {
                remainingNulls[remainingNullsCount++] = block.isNull(positions[positionsOffset + i]);
            }

            return;
        }

        // Process the remaining nulls from last batch
        int offset = positionsOffset;

        if (remainingNullsCount > 0) {
            byte value = 0;

            for (int i = 0; i < remainingNullsCount; i++) {
                value |= remainingNulls[i] ? 0b1000_0000 >>> i : 0;
            }

            // process a few more nulls to make up one byte
            for (int i = remainingNullsCount; i < BITS_IN_BYTE; i++) {
                value |= block.isNull(positions[offset++]) ? 0b1000_0000 >>> i : 0;
            }

            hasEncodedNulls |= (value != 0);
            nullsBufferIndex = setByteUnchecked(nullsBuffer, nullsBufferIndex, value);

            remainingNullsCount = 0;
        }

        // Process the next BITS_IN_BYTE * n positions. We now have processed (offset - positionsOffset) positions
        int remainingPositions = batchSize - (offset - positionsOffset);
        int positionsToEncode = remainingPositions & ~0b111;
        for (int i = 0; i < positionsToEncode; i += BITS_IN_BYTE) {
            byte value = 0;
            value |= block.isNull(positions[offset]) ? 0b1000_0000 : 0;
            value |= block.isNull(positions[offset + 1]) ? 0b0100_0000 : 0;
            value |= block.isNull(positions[offset + 2]) ? 0b0010_0000 : 0;
            value |= block.isNull(positions[offset + 3]) ? 0b0001_0000 : 0;
            value |= block.isNull(positions[offset + 4]) ? 0b0000_1000 : 0;
            value |= block.isNull(positions[offset + 5]) ? 0b0000_0100 : 0;
            value |= block.isNull(positions[offset + 6]) ? 0b0000_0010 : 0;
            value |= block.isNull(positions[offset + 7]) ? 0b0000_0001 : 0;

            hasEncodedNulls |= (value != 0);
            nullsBufferIndex = setByteUnchecked(nullsBuffer, nullsBufferIndex, value);
            offset += BITS_IN_BYTE;
        }

        // Process the remaining positions
        remainingNullsCount = remainingPositions & 0b111;
        for (int i = 0; i < remainingNullsCount; i++) {
            remainingNulls[i] = block.isNull(positions[offset++]);
        }
    }

    private void encodeNonNullsAsBits(int count)
    {
        if (remainingNullsCount + count < BITS_IN_BYTE) {
            // just put all of this batch to remainingNulls
            for (int i = 0; i < count; i++) {
                remainingNulls[remainingNullsCount++] = false;
            }
            return;
        }

        int remainingPositions = count - encodeRemainingNullsAsBits();

        nullsBufferIndex = fill(nullsBuffer, nullsBufferIndex, remainingPositions >>> 3, (byte) 0);

        remainingNullsCount = remainingPositions & 0b111;
        Arrays.fill(remainingNulls, false);
    }

    private int encodeRemainingNullsAsBits()
    {
        if (remainingNullsCount == 0) {
            return 0;
        }

        byte value = 0;
        for (int i = 0; i < remainingNullsCount; i++) {
            value |= remainingNulls[i] ? 0b1000_0000 >>> i : 0;
        }

        hasEncodedNulls |= (value != 0);
        nullsBufferIndex = setByteUnchecked(nullsBuffer, nullsBufferIndex, value);

        int padding = BITS_IN_BYTE - remainingNullsCount;
        remainingNullsCount = 0;
        return padding;
    }

    protected long getNullsBufferRetainedSizeInBytes()
    {
        return sizeOf(nullsBuffer) + sizeOf(remainingNulls);
    }

    protected long getNullsBufferSerializedSizeInBytes()
    {
        long size = SIZE_OF_BYTE;       // nulls uses 1 byte for mayHaveNull
        if (containsNull()) {
            size += nullsBufferIndex +  // nulls buffer
                    (remainingNullsCount > 0 ? SIZE_OF_BYTE : 0);   // The remaining nulls not serialized yet. We have to add it here because at the time of calling this function, the remaining nulls was not put into the nullsBuffer yet.
        }
        return size;
    }

    protected long getPositionsRetainedSizeInBytes()
    {
        return sizeOf(positions) + sizeOf(mappedPositions);
    }
}
