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
package com.facebook.presto.orc.stream;

import com.facebook.presto.orc.checkpoint.BooleanStreamCheckpoint;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;

@SuppressWarnings("NarrowingCompoundAssignment")
public class BooleanInputStream
        implements ValueInputStream<BooleanStreamCheckpoint>
{
    private static final int HIGH_BIT_MASK = 0b1000_0000;
    private final ByteInputStream byteStream;
    private byte data;
    private int bitsInData;

    public BooleanInputStream(OrcInputStream byteStream)
    {
        this.byteStream = new ByteInputStream(byteStream);
    }

    private void readByte()
            throws IOException
    {
        checkState(bitsInData == 0);
        data = byteStream.next();
        bitsInData = 8;
    }

    public boolean nextBit()
            throws IOException
    {
        // read more data if necessary
        if (bitsInData == 0) {
            readByte();
        }

        // read bit
        boolean result = (data & HIGH_BIT_MASK) != 0;

        // mark bit consumed
        data <<= 1;
        bitsInData--;

        return result;
    }

    @Override
    public Class<BooleanStreamCheckpoint> getCheckpointType()
    {
        return BooleanStreamCheckpoint.class;
    }

    @Override
    public void seekToCheckpoint(BooleanStreamCheckpoint checkpoint)
            throws IOException
    {
        byteStream.seekToCheckpoint(checkpoint.getByteStreamCheckpoint());
        bitsInData = 0;
        skip(checkpoint.getOffset());
    }

    @Override
    public void skip(long items)
            throws IOException
    {
        if (bitsInData >= items) {
            data <<= items;
            bitsInData -= items;
        }
        else {
            items -= bitsInData;
            bitsInData = 0;

            byteStream.skip(items >>> 3);
            items &= 0b111;

            if (items != 0) {
                readByte();
                data <<= items;
                bitsInData -= items;
            }
        }
    }

    public int countBitsSet(int items)
            throws IOException
    {
        int count = 0;

        // count buffered data
        if (items > bitsInData && bitsInData > 0) {
            count += bitCount(data);
            items -= bitsInData;
            bitsInData = 0;
        }

        // count whole bytes
        while (items > 8) {
            count += bitCount(byteStream.next());
            items -= 8;
        }

        // count remaining bits
        for (int i = 0; i < items; i++) {
            count += nextBit() ? 1 : 0;
        }

        return count;
    }

    /**
     * Sets the vector element to true if the bit is set.
     */
    public int getSetBits(int batchSize, boolean[] vector)
            throws IOException
    {
        int count = 0;
        int bitsInFirstByte = Math.min(batchSize, bitsInData);
        for (int i = 0; i < bitsInFirstByte; i++) {
            vector[i] = nextBit();
            count += vector[i] ? 1 : 0;
        }
        verify(bitsInData == 0);
        int wholeBytes = (batchSize - bitsInFirstByte) / 8;
        int numFilled = bitsInFirstByte;
        for (int i = 0; i < wholeBytes; i++) {
            byte data = byteStream.next();
            count += Integer.bitCount(data);
            vector[numFilled] = (data & 0x80) != 0;
            vector[numFilled + 1] = (data & 0x40) != 0;
            vector[numFilled + 2] = (data & 0x20) != 0;
            vector[numFilled + 3] = (data & 0x10) != 0;
            vector[numFilled + 4] = (data & 0x08) != 0;
            vector[numFilled + 5] = (data & 0x04) != 0;
            vector[numFilled + 6] = (data & 0x02) != 0;
            vector[numFilled + 7] = (data & 0x01) != 0;
            numFilled += 8;
        }
        for (int i = numFilled; i < batchSize; i++) {
            vector[i] = nextBit();
            count += vector[i] ? 1 : 0;
        }
        return count;
    }

    // vector[i] is set to the bit at offsets[i] - offsetBase for i
    // from 0 to numOffsets. The offset[i] - offsetBase values are
    // relative to the current position of the stream. 0 would mean
    // the value returned by a call to next(). The stream is advanced
    // by numBits. The offset values are increasing and non-repeating.
    public void getSetBits(int[] offsets, int numOffsets, int offsetBase, int numBits, boolean[] vector)
            throws IOException
    {
        if (offsets[0] == offsetBase && offsets[numOffsets - 1] == offsetBase + numOffsets - 1) {
            getSetBits(numOffsets, vector);
            if (numBits > offsets[numOffsets - offsetBase - 1] + 1) {
                skip(numBits - offsets[numOffsets - 1] - offsetBase - 1);
            }
            return;
        }
        int position = 0;
        for (int i = 0; i < numOffsets; i++) {
            int target = offsets[i] - offsetBase;
            if (target > position) {
                skip(target - position);
                position = target;
            }
            vector[i] = nextBit();
            position++;
        }
        if (numBits > offsets[numOffsets - 1] - offsetBase) {
            skip(offsets[numOffsets - 1] - offsetBase - position);
        }
    }

    /**
     * Sets the vector element to true if the bit is set, skipping the null values.
     */
    public int getSetBits(int batchSize, boolean[] vector, boolean[] isNull)
            throws IOException
    {
        int count = 0;
        for (int i = 0; i < batchSize; i++) {
            if (!isNull[i]) {
                vector[i] = nextBit();
                count += vector[i] ? 1 : 0;
            }
        }
        return count;
    }

    /**
     * Sets the vector element to true if the bit is set.
     */
    public void getSetBits(Type type, int batchSize, BlockBuilder builder)
            throws IOException
    {
        for (int i = 0; i < batchSize; i++) {
            type.writeBoolean(builder, nextBit());
        }
    }

    /**
     * Sets the vector element to true if the bit is not set.
     */
    public int getUnsetBits(int batchSize, boolean[] vector)
            throws IOException
    {
        return getUnsetBits(batchSize, vector, 0);
    }

    /**
     * Sets the vector element to true for the batchSize number of elements starting at offset
     * if the bit is not set.
     */
    public int getUnsetBits(int batchSize, boolean[] vector, int offset)
            throws IOException
    {
        int count = 0;
        for (int i = offset; i < batchSize + offset; i++) {
            vector[i] = !nextBit();
            count += vector[i] ? 1 : 0;
        }
        return count;
    }

    /**
     * Return the number of unset bits
     */
    public int getUnsetBits(int batchSize)
            throws IOException
    {
        int count = 0;
        for (int i = 0; i < batchSize; i++) {
            count += nextBit() ? 0 : 1;
        }
        return count;
    }

    private static int bitCount(byte data)
    {
        return Integer.bitCount(data & 0xFF);
    }
}
