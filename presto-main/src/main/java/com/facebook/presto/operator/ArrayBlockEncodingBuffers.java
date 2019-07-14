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

import com.facebook.presto.spi.block.ColumnarArray;
import com.google.common.annotations.VisibleForTesting;
import io.airlift.slice.SliceOutput;
import org.openjdk.jol.info.ClassLayout;

import static com.facebook.presto.array.Arrays.ensureCapacity;
import static com.facebook.presto.operator.UncheckedByteArrays.setIntUnchecked;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;
import static sun.misc.Unsafe.ARRAY_INT_INDEX_SCALE;

public class ArrayBlockEncodingBuffers
        extends BlockEncodingBuffers
{
    @VisibleForTesting
    public static final int POSITION_SIZE = SIZE_OF_INT + SIZE_OF_BYTE;

    private static final String NAME = "ARRAY";
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ArrayBlockEncodingBuffers.class).instanceSize();

    // The buffer for the offsets for all incoming blocks so far
    private byte[] offsetsBuffer;

    // The address that the next offset value will be written to.
    private int offsetsBufferIndex;

    // This array holds the condensed offsets for each position for the incoming block.
    private int[] offsets;

    // The last offset in the offsets buffer
    private int lastOffset;

    // This array holds the offsets into its nested raw block for each top level row.
    private int[] offsetsCopy;

    private final BlockEncodingBuffers elementsBuffers;

    public ArrayBlockEncodingBuffers(DecodedBlockNode decodedBlockNode)
    {
        elementsBuffers = createBlockEncodingBuffers(decodedBlockNode.getChildren().get(0));
    }

    @Override
    public void resetBuffers()
    {
        bufferedPositionCount = 0;
        offsetsBufferIndex = 0;
        lastOffset = 0;
        resetNullsBuffer();

        elementsBuffers.resetBuffers();
    }

    @Override
    protected void setupDecodedBlockAndMapPositions(DecodedBlockNode decodedBlockNode)
    {
        requireNonNull(decodedBlockNode, "decodedBlockNode is null");

        decodedBlockNode = mapPositions(decodedBlockNode);
        ColumnarArray columnarArray = (ColumnarArray) decodedBlockNode.getDecodedBlock();
        decodedBlock = columnarArray.getNullCheckBlock();

        populateNestedPositions(columnarArray);

        elementsBuffers.setupDecodedBlockAndMapPositions(decodedBlockNode.getChildren().get(0));
    }

    @Override
    public void accumulateRowSizes(int[] rowSizes)
    {
        for (int i = 0; i < positionCount; i++) {
            rowSizes[i] += POSITION_SIZE;
        }

        offsetsCopy = ensureCapacity(offsetsCopy, positionCount + 1);
        System.arraycopy(offsets, 0, offsetsCopy, 0, positionCount + 1);

        elementsBuffers.accumulateRowSizes(offsetsCopy, positionCount, rowSizes);
    }

    @Override
    protected void accumulateRowSizes(int[] positionOffsets, int positionCount, int[] rowSizes)
    {
        if (this.positionCount == 0) {
            return;
        }

        int lastOffset = positionOffsets[0];
        for (int i = 0; i < positionCount; i++) {
            int offset = positionOffsets[i + 1];
            rowSizes[i] += POSITION_SIZE * (offset - lastOffset);
            lastOffset = offset;
            positionOffsets[i + 1] = offsets[offset];
        }

        elementsBuffers.accumulateRowSizes(positionOffsets, positionCount, rowSizes);
    }

    @Override
    public void setNextBatch(int positionsOffset, int batchSize)
    {
        this.positionsOffset = positionsOffset;
        this.batchSize = batchSize;

        // The nested level positionCount could be 0.
        if (this.positionCount == 0) {
            return;
        }

        int offset = offsets[positionsOffset];

        elementsBuffers.setNextBatch(offset, offsets[positionsOffset + batchSize] - offset);
    }

    @Override
    public void copyValues()
    {
        if (batchSize == 0) {
            return;
        }

        appendNulls();
        appendOffsets();
        elementsBuffers.copyValues();

        bufferedPositionCount += batchSize;
    }

    @Override
    public void serializeTo(SliceOutput output)
    {
        writeLengthPrefixedString(output, NAME);

        elementsBuffers.serializeTo(output);

        output.writeInt(bufferedPositionCount);

        // offsets
        output.writeInt(0);  // the base position
        if (offsetsBufferIndex > 0) {
            output.appendBytes(offsetsBuffer, 0, offsetsBufferIndex);
        }

        serializeNullsTo(output);
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE +
                getPositionsRetainedSizeInBytes() +
                sizeOf(offsetsBuffer) +
                sizeOf(offsets) +
                sizeOf(offsetsCopy) +
                getNullsBufferRetainedSizeInBytes();
    }

    @Override
    public long getSerializedSizeInBytes()
    {
        return NAME.length() + SIZE_OF_INT +            // encoding name
                elementsBuffers.getSerializedSizeInBytes() +   // nested block
                SIZE_OF_INT +                           // positionCount
                SIZE_OF_INT +                           // offset 0. The offsetsBuffer doesn't contain the offset 0 so we need to add it here.
                offsetsBufferIndex +                    // offsets buffer.
                getNullsBufferSerializedSizeInBytes();  // nulls
    }

    private void populateNestedPositions(ColumnarArray columnarArray)
    {
        // Reset nested level positions before checking positionCount. Failing to do so may result in elementsBuffers having stale values when positionCount is 0.
        elementsBuffers.resetPositions();

        if (positionCount == 0) {
            return;
        }

        offsets = ensureCapacity(offsets, positionCount + 1);

        int[] positions = getPositions();

        int baseOffset = columnarArray.getOffset(0);
        for (int i = 0; i < positionCount; i++) {
            int position = positions[i];
            int beginOffset = columnarArray.getOffset(position);
            int endOffset = columnarArray.getOffset(position + 1);
            int length = endOffset - beginOffset;

            offsets[i + 1] = offsets[i] + length;

            if (length > 0) {
                // beginOffset is the absolute position in the nested block. We need to subtract the base offset from it to get the logical position.
                elementsBuffers.appendPositionRange(beginOffset - baseOffset, length);
            }
        }
    }

    private void appendOffsets()
    {
        offsetsBuffer = ensureCapacity(offsetsBuffer, offsetsBufferIndex + batchSize * ARRAY_INT_INDEX_SCALE, 2.0f, true);

        int baseOffset = lastOffset - offsets[positionsOffset];
        for (int i = positionsOffset; i < positionsOffset + batchSize; i++) {
            offsetsBufferIndex = setIntUnchecked(offsetsBuffer, offsetsBufferIndex, offsets[i + 1] + baseOffset);
        }
        lastOffset = offsets[positionsOffset + batchSize] + baseOffset;
    }
}
