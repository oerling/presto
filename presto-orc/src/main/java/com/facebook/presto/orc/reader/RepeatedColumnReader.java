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
// import com.facebook.presto.orc.stream.BooleanInputStream;
// import com.facebook.presto.orc.stream.LongInputStream;
// import com.facebook.presto.spi.block.Block;
// import com.facebook.presto.spi.type.Type;

// import java.io.IOException;
 import java.util.Arrays;
import java.util.OptionalInt;

import static com.google.common.base.Verify.verify;

abstract class RepeatedColumnReader
        extends NullWrappingColumnReader
{
    // Starting offset of each result in the element reader's Block.
    protected int[] elementOffset;
    // Length of each row in the input QualifyingSet.
    protected int[] elementLength;
    // Start of each row of inputQualifyingSet in the inner  data.
    protected int[] elementStart;
    // Filter to apply to the corresponding element of innerQualifyingSet.
    Filter[] elementFilter;

    // Number of filters for the corresponding element of the
    // inputQualifyingSet. If this is less than the number of filters
    // per element, then this means that the subscript of some filter
    // did not exist in this element. Thus, if this many filters
    // passed, we have an error because a missing subscript would have
    // been accessed.
    int[] numElementFilters;

    // Used for compactValues of repeated content.
    protected int[] innerSurviving;
    protected int numInnerSurviving;
    protected int innerSurvivingBase;

    // Number of rows of nested content read. This is after applying any pushed down filters.
    protected long innerRowCount;
    // Number of top level rows read.
    protected long outerRowCount;

    RepeatedColumnReader()
    {
        super(OptionalInt.empty());
    }

    protected int getInnerPosition(int position)
    {
        return elementOffset[position];
    }

    protected void computeInnerSurviving(int[]surviving, int base, int numSurviving)
    {
        innerSurvivingBase = elementOffset[base];
        if (numSurviving == 0) {
            numInnerSurviving = 0;
            return;
        }
        int numInner = 0;
        for (int i = 0; i < numSurviving; i++) {
            int position = surviving[i] + base;
            numInner += elementOffset[position + 1] - elementOffset[position];
        }
        if (innerSurviving == null || innerSurviving.length < numInner) {
            innerSurviving = new int[numInner];
        }
        numInnerSurviving = numInner;
        int fill = 0;
        for (int i = 0; i < numSurviving; i++) {
            int position = surviving[i] + base;
            int startIdx = elementOffset[position];
            int endIdx = elementOffset[position + 1];
            for (int innerPosition = startIdx; innerPosition < endIdx; innerPosition++) {
                innerSurviving[fill++] = innerPosition - innerSurvivingBase;
            }
        }
    }

    @Override
    protected void makeInnerQualifyingSet()
    {
        hasNulls = presentStream != null;
        int nonNullRowIdx = 0;
        boolean nonDeterministic = filter != null && !deterministicFilter;
        if (innerQualifyingSet == null) {
            innerQualifyingSet = new QualifyingSet();
        }
        int[] inputRows = inputQualifyingSet.getPositions();
        int numActive = inputQualifyingSet.getPositionCount();
        if (elementLength == null || elementLength.length < numActive) {
            elementLength = new int[numActive];
            elementStart = new int[numActive];
        }
        innerQualifyingSet.reset(countInnerActive());
        int prevRow = 0;
        int prevInner = innerPosInRowGroup;
        numNullsToAdd = 0;
        boolean keepNulls = filter == null || (!nonDeterministic && filter.testNull());
        for (int activeIdx = 0; activeIdx < numActive; activeIdx++) {
            int row = inputRows[activeIdx] - posInRowGroup;
            if (presentStream != null && !present[row]) {
                elementLength[activeIdx] = 0;
                elementStart[activeIdx] = prevInner;
                if (keepNulls || (nonDeterministic && testNullAt(row))) {
                    addNullToKeep(inputRows[activeIdx], activeIdx);
                }
            }
            else {
                prevInner += innerDistance(prevRow, row, nonNullRowIdx);
                nonNullRowIdx += countPresent(prevRow, row);
                prevRow = row;
                int length = lengths[nonNullRowIdx];
                elementLength[activeIdx] = length;
                elementStart[activeIdx] = prevInner;
                for (int i = 0; i < length; i++) {
                    innerQualifyingSet.append(prevInner + i, activeIdx);
                }
            }
        }
        numInnerRows = innerQualifyingSet.getPositionCount();
        int skip = innerDistance(prevRow, inputQualifyingSet.getEnd() - posInRowGroup, nonNullRowIdx);
        innerQualifyingSet.setEnd(skip + prevInner);
        skip = countPresent(prevRow, inputQualifyingSet.getEnd() - posInRowGroup);
        lengthIdx = nonNullRowIdx + skip;
    }

    // Returns the number of nested rows to skip to go from
    // 'outerBegin' to 'outerEnd'. 'outerBegin' and 'outerEnd' are
    // offsets from 'posInRowGroup' of the map/list
    // reader. nonNullRowIdx is the number of non-null map/list rows
    // before outerBegin.
    private int innerDistance(int outerBegin, int outerEnd, int nonNullRowIdx)
    {
        int distance = 0;
        int numPresent = countPresent(outerBegin, outerEnd);
        for (int ctr = 0; ctr < numPresent; ctr++) {
            distance += lengths[nonNullRowIdx + ctr];
        }
        return distance;
    }

    private int countInnerActive()
    {
        int[] inputRows = inputQualifyingSet.getPositions();
        int numActive = inputQualifyingSet.getPositionCount();
        int nonNullRowIdx = 0;
        int total = 0;
        int prevRow = 0;
        for (int i = 0; i < numActive; i++) {
            int row = inputRows[i] - posInRowGroup;
            if (presentStream != null && !present[row]) {
                continue;
            }
            int distance = countPresent(prevRow, row);
            nonNullRowIdx += distance;
            total += lengths[nonNullRowIdx];
            prevRow = row;
        }
        return total;
    }

@Override
    public void erase(int end)
    {
        if (outputChannel == -1 || numValues == 0) {
            return;
        }
        int innerEnd = getInnerPosition(end);
        numValues -= end;
        if (valueIsNull != null) {
            System.arraycopy(valueIsNull, end, valueIsNull, 0, numValues);
        }
        System.arraycopy(elementOffset, end, elementOffset, 0, numValues);
        for (int i = 0; i < numValues; i++) {
            verify(elementOffset[i] >= innerEnd);
            elementOffset[i] -= innerEnd;
        }
        eraseContent(innerEnd);
    }

    @Override
    public void compactValues(int[] surviving, int base, int numSurviving)
    {
        if (outputChannel != -1) {
            computeInnerSurviving(surviving, base, numSurviving);
            int elementBase = getInnerPosition(base);
            for (int i = 0; i < numSurviving; i++) {
                int survivingRow = surviving[i] + base;
                if (valueIsNull != null && valueIsNull[survivingRow]) {
                    valueIsNull[base + i] = true;
                    elementOffset[base + i] = elementBase;
                }
                else {
                    if (valueIsNull != null) {
                        valueIsNull[base + i] = false;
                    }
                    elementOffset[base + i] = elementBase;
                    elementBase += elementOffset[survivingRow + 1] - elementOffset[survivingRow];
                }
            }
            elementOffset[base + numSurviving] = elementBase;
            compactContent(innerSurviving, innerSurvivingBase, numInnerSurviving);
            numValues = base + numSurviving;
        }
        compactQualifyingSet(surviving, numSurviving);
    }
    protected abstract void eraseContent(int innerEnd);

    protected abstract void compactContent(int[] innerSurviving, int innerSurvivingDase, int numInnerSurviving);

    protected void ensureValuesCapacity(int numAdded)
    {
        int newSize = numValues + numAdded * 2;
        if (presentStream != null && valueIsNull == null) {
            valueIsNull = new boolean[newSize];
        }
        if (valueIsNull != null && valueIsNull.length < numValues + numAdded) {
            valueIsNull = Arrays.copyOf(valueIsNull, newSize);
        }
        if (elementOffset == null) {
            elementOffset = new int[newSize];
        }
        else if (elementOffset.length < numValues + numAdded + 1) {
            elementOffset = Arrays.copyOf(elementOffset, newSize);
        }
    }

}
