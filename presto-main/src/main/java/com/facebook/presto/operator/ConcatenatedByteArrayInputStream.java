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

import io.airlift.slice.ByteArrays;
import io.airlift.slice.Slice;
import io.airlift.slice.FixedLengthSliceInput;
import io.airlift.slice.Slices;
import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_FLOAT;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.SIZE_OF_SHORT;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

// Implements a seekable SliceInput over a list of byte[]. When
// freeAfterRead is set, when a read reaches or skips past the end of
// a byte array, the the reference of the array is dropped and it is
// returned to an allocator.
public final class ConcatenatedByteArrayInputStream
        extends FixedLengthSliceInput
{
    public interface Allocator
    {
        void free(byte[] bytes);
    }

    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ConcatenatedByteArrayInputStream.class).instanceSize();

    private ArrayList<byte[]> buffers;
    private byte[] currentBuffer;
    private int currentBufferIndex;
    private long currentBufferSize;
    private long position;
    private long totalSize;
    private final Allocator allocator;
    private boolean freeAfterRead;
    private boolean freeAfterSubstreamsFinish;
    private byte[] tempBytes = new byte[SIZE_OF_LONG];
    private long previousBuffersSize;
    private ConcatenatedByteArrayInputStream parent;
    private int substreamCount;

    public ConcatenatedByteArrayInputStream(List<byte[]> buffers, long size, Allocator allocator)
    {
        requireNonNull(buffers);
        long buffersSize = 0;
        this.buffers = new ArrayList();
        for (byte[] buffer : buffers) {
            this.buffers.add(buffer);
            buffersSize += buffer.length;
        }
        totalSize = size;
        this.allocator = allocator;
        if (totalSize <= buffersSize - buffers.get(buffers.size()  - 1).length || totalSize > buffersSize) {
            throw new IllegalArgumentException("totalSize is not an offset that falls within the last buffer.");
        }
        position = 0;
        currentBufferIndex = -1;
        nextBuffer(0);
    }

    private ConcatenatedByteArrayInputStream(ConcatenatedByteArrayInputStream parent, long size)
    {
        long buffersSize = 0;
        this.parent = parent;
        allocator = null;
        this.buffers = new ArrayList();
        for (byte[] buffer : parent.buffers) {
            this.buffers.add(buffer);
            buffersSize += buffer.length;
            if (buffersSize >= size) {
                break;
            }
        }
        totalSize = size;
        position = 0;
        currentBufferIndex = -1;
        nextBuffer(0);
    }

    // Returns a stream giving access to the contents of this. The same set of buffers may be accessed from multiple threads via different substreams.
    public ConcatenatedByteArrayInputStream getSubstream(long end)
    {
        if (parent != null) {
            throw new IllegalArgumentException("Only one level of substreams is supported");
        }
        substreamCount++;
        return new ConcatenatedByteArrayInputStream(this, end);
    }

    public void setFreeAfterRead()
    {
        freeAfterRead = true;
    }

    public void setFreeAfterSubstreamsFinish()
    {
        freeAfterSubstreamsFinish = true;
    }

    private void nextBuffer(int dataSize)
    {
        int numInCurrent = 0;
        if (dataSize > 0) {
            numInCurrent = toIntExact(currentBufferSize - position);
            System.arraycopy(currentBuffer, toIntExact(position), tempBytes, 0, numInCurrent);
        }
        if (currentBufferIndex >= 0 && freeAfterRead) {
            if (allocator != null && substreamCount == 0 && parent == null) {
                allocator.free(buffers.get(currentBufferIndex));
            }
                buffers.set(currentBufferIndex, null);
        }
        previousBuffersSize += currentBufferSize;
        currentBufferIndex++;
        if (currentBufferIndex >= buffers.size()) {
            if (dataSize - numInCurrent > 0) {
                throw new IndexOutOfBoundsException();
            }
            if (parent != null) {
                parent.substreamFinished();
            }
            currentBuffer = null;
            currentBufferSize = 0;
            return;
        }
        currentBuffer = buffers.get(currentBufferIndex);
        if (currentBufferIndex == buffers.size() - 1) {
            currentBufferSize = totalSize - previousBuffersSize;
        }
        else {
            currentBufferSize = currentBuffer.length;
        }
        position = 0;
        if (dataSize > numInCurrent) {
            System.arraycopy(currentBuffer, 0, tempBytes, numInCurrent, dataSize - numInCurrent);
            position = dataSize - numInCurrent;
        }
    }

    private void substreamFinished()
    {
        boolean finished;
        synchronized (this) {
            finished = --substreamCount == 0;
        }
        if (finished) {
            if (allocator != null) {
                for (byte[] buffer : buffers) {
                    allocator.free(buffer);
                }
            }
            buffers = null;
        }
    }

    @Override
    public long length()
    {
        return totalSize;
    }

    @Override
    public long position()
    {
        return previousBuffersSize + position;
    }

    @Override
    public void setPosition(long position)
    {
        boolean isFinalRead = freeAfterRead && buffers.get(0) == null;
        if ((isFinalRead && position < previousBuffersSize) || position > totalSize) {
            throw new IndexOutOfBoundsException();
        }
        if (position > previousBuffersSize && position - previousBuffersSize < currentBufferSize) {
            this.position = position - previousBuffersSize;
            return;
        }
        if (!isFinalRead) {
            previousBuffersSize = 0;
        }
        for (int i = 0; i < buffers.size(); i++) {
            byte[] buffer = buffers.get(i);
            if (buffer == null) {
                continue;
            }
            currentBufferIndex = i;
            currentBuffer = buffer;
            currentBufferSize = buffer.length;
            if (position >= previousBuffersSize && position < previousBuffersSize + currentBufferSize) {
                this.position = position - previousBuffersSize;
                return;
            }
            nextBuffer(0);
        }
        throw new IndexOutOfBoundsException();
    }

    @Override
    public boolean isReadable()
    {
        return position < currentBufferSize || currentBufferIndex < buffers.size() - 1;
    }

    @Override
    public int available()
    {
        int avail = toIntExact(totalSize - previousBuffersSize - position);
        if (avail < 0) {
            throw new IllegalArgumentException("Reading past end");
        }
        return avail;
    }

    public int contiguousAvailable()
    {
        return toIntExact(currentBufferSize - position);
    }

    public byte[] getBuffer()
    {
        return currentBuffer;
    }

    public int getOffsetInBuffer()
    {
        return toIntExact(position);
    }

    @Override
    public boolean readBoolean()
    {
        return readByte() != 0;
    }

    @Override
    public int read()
    {
        if (position >= currentBufferSize) {
            nextBuffer(0);
            if (currentBuffer == null) {
                return -1;
            }
        }
        int result = currentBuffer[toIntExact(position)] & 0xff;
        position++;
        return result;
    }

    @Override
    public byte readByte()
    {
        int value = read();
        if (value == -1) {
            throw new IndexOutOfBoundsException();
        }
        return (byte) value;
    }

    @Override
    public int readUnsignedByte()
    {
        return readByte() & 0xFF;
    }

    @Override
    public short readShort()
    {
        long newPosition = position + SIZE_OF_SHORT;
        short v;
        if (newPosition < currentBufferSize) {
            v = ByteArrays.getShort(currentBuffer, (int) position);
            position = newPosition;
        }
        else {
            nextBuffer(SIZE_OF_SHORT);
            v = ByteArrays.getShort(tempBytes, 0);
        }
        return v;
    }

    @Override
    public int readUnsignedShort()
    {
        return readShort() & 0xFFFF;
    }

    @Override
    public int readInt()
    {
        long newPosition = position + SIZE_OF_INT;
        int v;
        if (newPosition < currentBufferSize) {
            v = ByteArrays.getInt(currentBuffer, (int) position);
            position = newPosition;
        }
        else {
            nextBuffer(SIZE_OF_INT);
            v = ByteArrays.getInt(tempBytes, 0);
        }
        return v;
    }

    @Override
    public long readLong()
    {
        long newPosition = position + SIZE_OF_LONG;
        long v;
        if (newPosition < currentBufferSize) {
            v = ByteArrays.getLong(currentBuffer, (int) position);
            position = newPosition;
        }
        else {
            nextBuffer(SIZE_OF_LONG);
            v = ByteArrays.getLong(tempBytes, 0);
        }
        return v;
    }

    @Override
    public float readFloat()
    {
        long newPosition = position + SIZE_OF_FLOAT;
        float v;
        if (newPosition < currentBufferSize) {
            v = ByteArrays.getFloat(currentBuffer, (int) position);
            position = newPosition;
        }
        else {
            nextBuffer(SIZE_OF_FLOAT);
            v = ByteArrays.getFloat(tempBytes, 0);
        }
        return v;
    }

    @Override
    public double readDouble()
    {
        long newPosition = position + SIZE_OF_DOUBLE;
        double v;
        if (newPosition < currentBufferSize) {
            v = ByteArrays.getDouble(currentBuffer, (int) position);
            position = newPosition;
        }
        else {
            nextBuffer(SIZE_OF_DOUBLE);
            v = ByteArrays.getFloat(tempBytes, 0);
        }
        return v;
    }

    @Override
    public Slice readSlice(int length)
    {
        if (length == 0) {
            return Slices.EMPTY_SLICE;
        }
        if (!freeAfterRead && currentBufferSize - position <= length) {
            Slice v = Slices.wrappedBuffer(currentBuffer, (int) position, length);
            skip(length);
            return v;
        }
        byte[] bytes = new byte[length];
        readBytes(bytes, 0, length);
        return Slices.wrappedBuffer(bytes);
    }

    @Override
    public int read(byte[] destination, int destinationIndex, int length)
    {
        if (length == 0) {
            return 0;
        }
        length = Math.min(length, available());
        if (length == 0) {
            return -1;
        }
        readBytes(destination, destinationIndex, length);
        return length;
    }

    @Override
    public void readBytes(byte[] destination, int destinationIndex, int length)
    {
        while (length > 0) {
            int copy = (int) Math.min(length, currentBufferSize - position);
            if (copy == 0) {
                nextBuffer(0);
                if (currentBuffer == null) {
                    throw new IndexOutOfBoundsException();
                }
                continue;
            }
            System.arraycopy(currentBuffer, (int) position, destination, destinationIndex, copy);
            position += copy;
            destinationIndex += copy;
            length -= copy;
        }
        if (position == currentBufferSize) {
            nextBuffer(0);
        }
    }

    @Override
    public void readBytes(Slice destination, int destinationIndex, int length)
    {
        while (length > 0) {
            int copy = (int) Math.min(length, currentBufferSize - position);
            if (copy == 0) {
                nextBuffer(0);
                if (currentBuffer == null) {
                    throw new IndexOutOfBoundsException();
                }
                continue;
            }
            destination.setBytes(destinationIndex, currentBuffer, (int) position, copy);
            position += copy;
            destinationIndex += copy;
            length -= copy;
        }
        if (position == currentBufferSize) {
            nextBuffer(0);
        }
    }

    @Override
    public void readBytes(OutputStream out, int length)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long skip(long length)
    {
        length = Math.min(length, available());
        long toGo = length;
        while (toGo > 0) {
            if (toGo < currentBufferSize - position) {
                position += toGo;
                break;
            }
            toGo -= currentBufferSize - position;
            nextBuffer(0);
        }
        return length;
    }

    @Override
    public int skipBytes(int length)
    {
        return (int) skip(length);
    }

    @Override
    public long getRetainedSize()
    {
        return INSTANCE_SIZE + totalSize;
    }

    /**
     * Returns a slice of this buffer's readable bytes. Modifying the content
     * of the returned buffer or this buffer affects each other's content
     * while they maintain separate indexes and marks.  This method is
     * identical to {@code buf.slice(buf.position(), buf.available()())}.
     * This method does not modify {@code position} or {@code writerIndex} of
     * this buffer.
     */
    public Slice slice()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Decodes this buffer's readable bytes into a string with the specified
     * character set name.  This method is identical to
     * {@code buf.toString(buf.position(), buf.available()(), charsetName)}.
     * This method does not modify {@code position} or {@code writerIndex} of
     * this buffer.
     */
    public String toString(Charset charset)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder("ConcatenatedByteArrayInputStream{");
        builder.append("position=").append(previousBuffersSize + position);
        builder.append(", capacity=").append(totalSize);
        builder.append('}');
        return builder.toString();
    }
}
