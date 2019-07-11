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

import com.facebook.presto.orc.Filter;
import com.facebook.presto.orc.QualifyingSet;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.stream.InputStreamSources;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.OptionalInt;

import static com.facebook.presto.orc.ResizedArrays.newIntArrayForReuse;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static com.google.common.base.Verify.verify;
import static java.lang.Double.longBitsToDouble;
import static java.lang.Float.intBitsToFloat;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

public class BlockStreamReader
        extends ColumnReader
{
    private Block block;
    int[] livePositions;

    public BlockStreamReader()
    {
        super(OptionalInt.empty());
    }

    public void setBlock(Block block)
    {
        this.block = block;
    }

    @Override
    public void startRowGroup(InputStreamSources dataStreamSources)
    {
    }

    @Override
    public void startStripe(InputStreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
            throws IOException
    {
    }

    @Override
    public void prepareNextRead(int batchSize)
    {
    }

    @Override
    public Block readBlock(Type type)
            throws IOException
    {
        return null;
    }

    @Override
    public void erase(int end)
    {
    }

    @Override
    public void compactValues(int[] surviving, int base, int numSurviving)
    {
        verify(base == 0);
        if (outputChannelSet) {
            if (livePositions == null) {
                livePositions = Arrays.copyOf(surviving, numSurviving);
            }
            else {
                for (int i = 0; i < numSurviving; i++) {
                    livePositions[i] = livePositions[surviving[i]];
                }
            }
            numValues = numSurviving;
        }
        compactQualifyingSet(surviving, numSurviving);
    }

    @Override
    public void scan()
            throws IOException
    {
        beginScan(null, null);
        if (filter == null) {
            numValues = block.getPositionCount();
            setLivePositions(inputQualifyingSet.getPositions(), inputQualifyingSet.getPositionCount());
            return;
        }
        filterBlock(block, type, filter, inputQualifyingSet, outputQualifyingSet);
        if (outputChannelSet) {
            setLivePositions(outputQualifyingSet.getPositions(), outputQualifyingSet.getPositionCount());
        }
    }

    private void setLivePositions(int[] positions, int numPositions)
    {
        if (livePositions == null || livePositions.length < numPositions) {
            livePositions = newIntArrayForReuse(numPositions);
        }
        System.arraycopy(positions, 0, livePositions, 0, numPositions);
        numValues = numPositions;
    }

    @Override
    public Block getBlock(int numFirstRows, boolean mayReuse)
    {
        if (numValues == numFirstRows && block.getPositionCount() == numValues) {
            return block;
        }
        return block.getPositions(livePositions, 0, numValues);
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return block == null ? 0 : block.getRetainedSizeInBytes();
    }

    @Override
    public void close()
    {
    }

    public static void filterBlock(Block block, Type type, Filter filter, QualifyingSet input, QualifyingSet output)
    {
        int numInput = input.getPositionCount();
        output.reset(numInput);
        int[] activeRows = input.getPositions();
        if (type == BIGINT || type == INTEGER || type == SMALLINT) {
            for (int i = 0; i < numInput; i++) {
                int position = activeRows[i];
                if (block.isNull(position)) {
                    if (filter.testNull()) {
                        output.append(position, i);
                    }
                }
                else if (filter.testLong(type.getLong(block, position))) {
                    output.append(position, i);
                }
            }
        }
        else if (type == DOUBLE) {
            for (int i = 0; i < numInput; i++) {
                int position = activeRows[i];
                if (block.isNull(position)) {
                    if (filter.testNull()) {
                        output.append(position, i);
                    }
                }
                else if (filter.testDouble(longBitsToDouble(block.getLong(position)))) {
                    output.append(position, i);
                }
            }
        }
        else if (type == REAL) {
            for (int i = 0; i < numInput; i++) {
                int position = activeRows[i];
                if (block.isNull(position)) {
                    if (filter.testNull()) {
                        output.append(position, i);
                    }
                }
                else if (filter.testFloat(intBitsToFloat(block.getInt(position)))) {
                    output.append(position, i);
                }
            }
        }
        else if (isVarcharType(type)) {
            for (int i = 0; i < numInput; i++) {
                int position = activeRows[i];
                if (block.isNull(position)) {
                    if (filter.testNull()) {
                        output.append(position, i);
                    }
                }
                else {
                    Slice slice = block.getSlice(position, 0, block.getSliceLength(position));
                    if (filter.testBytes((byte[]) slice.getBase(), (int) slice.getAddress() - ARRAY_BYTE_BASE_OFFSET, slice.length())) {
                        output.append(position, i);
                    }
                }
            }
        }
        else {
            throw new UnsupportedOperationException("BlockStreamReadre of " + type.toString() + " not supported");
        }
    }

    @Override
    public int getResultSizeInBytes()
    {
        if (numValues == 0) {
            return 0;
        }
        return (int) block.getSizeInBytes();
    }

    @Override
    public int getAverageResultSize()
    {
        // This serves for budgeting and since there is no
        // freedp,freedom of choice for the batch, as this is set by
        // the LazyBlock, we return a constant.
        return 8;
    }
}
