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

import com.facebook.presto.spi.block.AbstractVariableWidthBlock;
import com.google.common.annotations.VisibleForTesting;
import io.airlift.slice.SliceOutput;
import org.openjdk.jol.info.ClassLayout;

import static com.facebook.presto.array.Arrays.ensureCapacity;
import static com.facebook.presto.operator.UncheckedByteArrays.setIntUnchecked;
import static com.facebook.presto.spi.MoreByteArrays.setBytes;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.sizeOf;
import static sun.misc.Unsafe.ARRAY_INT_INDEX_SCALE;

public class VariableWidthBlockEncodingBuffers
        extends BlockEncodingBuffers
{
    @VisibleForTesting
    public static final int POSITION_SIZE = Integer.BYTES + Byte.BYTES;

    private static final String NAME = "VARIABLE_WIDTH";
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(VariableWidthBlockEncodingBuffers.class).instanceSize();
    private static final int ESTIMATED_ELEMENT_SIZE = 8;

    // The buffer for the slice for all incoming blocks so far
    private byte[] sliceBuffer;

    // The address that the next slice will be written to.
    private int sliceBufferIndex;

    // The buffer for the offsets for all incoming blocks so far
    private byte[] offsetsBuffer;

    // The address that the next offset value will be written to.
    private int offsetsBufferIndex;

    // The last offset in the offsets buffer
    private int lastOffset;

    @Override
    public void resetBuffers()
    {
        bufferedPositionCount = 0;
        sliceBufferIndex = 0;
        offsetsBufferIndex = 0;
        lastOffset = 0;
        resetNullsBuffer();
    }

    @Override
    public void accumulateRowSizes(int[] rowSizes)
    {
        int[] positions = getPositions();
        for (int i = 0; i < positionCount; i++) {
            rowSizes[i] += POSITION_SIZE + decodedBlock.getSliceLength(positions[i]);
        }
    }

    @Override
    protected void accumulateRowSizes(int[] positionOffsets, int positionCount, int[] rowSizes)
    {
        // The nested level positionCount could be 0.
        if (this.positionCount == 0) {
            return;
        }

        int[] positions = getPositions();

        for (int i = 0; i < positionCount; i++) {
            for (int j = positionOffsets[i]; j < positionOffsets[i + 1]; j++) {
                rowSizes[i] += POSITION_SIZE + decodedBlock.getSliceLength(positions[j]);
            }
        }
    }

    @Override
    public void copyValues()
    {
        if (batchSize == 0) {
            return;
        }

        appendOffsetsAndSlices();
        appendNulls();
        bufferedPositionCount += batchSize;
    }

    @Override
    public void serializeTo(SliceOutput output)
    {
        writeLengthPrefixedString(output, NAME);

        output.writeInt(bufferedPositionCount);

        // offsets
        // note that VariableWidthBlock doesn't write the initial offset 0
        if (offsetsBufferIndex > 0) {
            output.appendBytes(offsetsBuffer, 0, offsetsBufferIndex);
        }

        // nulls
        serializeNullsTo(output);

        // slice
        output.writeInt(sliceBufferIndex);  // totalLength
        if (sliceBufferIndex > 0) {
            output.appendBytes(sliceBuffer, 0, sliceBufferIndex);
        }
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE +
                getPositionsRetainedSizeInBytes() +
                sizeOf(offsetsBuffer) +
                getNullsBufferRetainedSizeInBytes() +
                sizeOf(sliceBuffer);
    }

    @Override
    public long getSerializedSizeInBytes()
    {
        return NAME.length() + SIZE_OF_INT +    // NAME
                SIZE_OF_INT +                   // positionCount
                offsetsBufferIndex +            // offsets buffer.
                SIZE_OF_INT +                   // sliceBuffer size.
                sliceBufferIndex +               // sliceBuffer
                getNullsBufferSerializedSizeInBytes();  // nulls
    }

    // This implementation requires variableWidthBlock.getPositionOffset() to be public but it's much faster than the below one
    private void appendOffsetsAndSlices()
    {
        offsetsBuffer = ensureCapacity(offsetsBuffer, offsetsBufferIndex + batchSize * ARRAY_INT_INDEX_SCALE, 2.0f, true);

        AbstractVariableWidthBlock variableWidthBlock = (AbstractVariableWidthBlock) decodedBlock;
        int[] positions = getPositions();

        // This implementation uses variableWidthBlock.getPositionOffset() to achieve high performance
        int sliceLength = variableWidthBlock.getPositionOffset(variableWidthBlock.getPositionCount()) - variableWidthBlock.getPositionOffset(0);

        byte[] sliceBase = (byte[]) variableWidthBlock.getSlice(0, 0, sliceLength).getBase();

        for (int i = positionsOffset; i < positionsOffset + batchSize; i++) {
            int position = positions[i];
            int beginOffset = variableWidthBlock.getPositionOffset(position);
            int endOffset = variableWidthBlock.getPositionOffset(position + 1);
            int length = endOffset - beginOffset;

            lastOffset += length;
            offsetsBufferIndex = setIntUnchecked(offsetsBuffer, offsetsBufferIndex, lastOffset);

            if (length > 0) {
                sliceBuffer = ensureCapacity(sliceBuffer, sliceBufferIndex + length, 2.0f, true);
                sliceBufferIndex = setBytes(sliceBuffer, sliceBufferIndex, sliceBase, beginOffset, length);
            }
        }
    }
}
