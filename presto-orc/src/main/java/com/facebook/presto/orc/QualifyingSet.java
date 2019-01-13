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
package com.facebook.presto.orc;

import com.facebook.presto.spi.PageSourceOptions.ErrorSet;

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;

public class QualifyingSet
{
    // begin and end define the range of rows coverd. If a row >=
    // begin and < end and is not in positions rangeBegins[i] <= row <
    // rangeEnds[i] then row is not in the qualifying set.
    //private int begin;
    private int end;
    //private int[] rangeBegins;
    //private int[] rangeEnds;
    private int[] positions;
    //private int numRanges;
    private int positionCount;
    // Index into positions for the first row after truncation. -1 if
    // no truncation.
    private int truncationPosition = -1;

    private int[] inputNumbers;
    private boolean isRanges;
    private ErrorSet errorSet;

    static volatile int[] wholeRowGroup;
    static volatile int[] allZeros;
    private int[] ownedPositions;
    private int[] ownedInputNumbers;
    private QualifyingSet parent;

    static {
        wholeRowGroup = new int[10000];
        allZeros = new int[10000];
        Arrays.fill(allZeros, 0);
        for (int i = 0; i < 10000; i++) {
            wholeRowGroup[i] = i;
        }
    }

    public void setRange(int begin, int end)
    {
        //this.begin = begin;
        this.end = end;
        int[] zeros = allZeros;
        if (zeros.length < end - begin) {
            int[] newZeros = new int[end - begin];
            Arrays.fill(newZeros, 0);
            allZeros = newZeros;
            inputNumbers = newZeros;
        }
        else {
            inputNumbers = zeros;
        }
        if (begin == 0) {
            int[] rowGroup = wholeRowGroup;
            if (rowGroup.length >= end) {
                positions = rowGroup;
            }
            else {
                // Thread safe.  If many concurrently create a new wholeRowGroup, many are created but all but one become garbage and everybody has a right size array.
                int[] newWholeRowGroup = new int[end];
                for (int i = 0; i < end; i++) {
                    newWholeRowGroup[i] = i;
                }
                positions = newWholeRowGroup;
                wholeRowGroup = newWholeRowGroup;
            }
            positionCount = end;
        }
        else {
            if (ownedPositions == null || ownedPositions.length < end - begin) {
                ownedPositions = new int[(int) ((end - begin) * 1.2)];
            }
            positions = ownedPositions;

            for (int i = begin; i < end; i++) {
                positions[i - begin] = i;
            }
        }
    }

    public boolean isEmpty()
    {
        return positionCount == 0;
    }

    public int[] getPositions()
    {
        return positions;
    }

    public int[] getInputNumbers()
    {
        return inputNumbers;
    }

    public int[] getMutablePositions(int minSize)
    {
        if (positions == null || ownedPositions == null || ownedPositions.length < minSize) {
            minSize = (int) (minSize * 1.2);
            if (positions != null) {
                ownedPositions = Arrays.copyOf(positions, minSize);
            }
            else {
                ownedPositions = new int[minSize];
            }
            positions = ownedPositions;
        }
        else {
            System.arraycopy(positions, 0, ownedPositions, 0, positionCount);
            positions = ownedPositions;
        }
        return positions;
    }

    public int[] getMutableInputNumbers(int minSize)
    {
        if (inputNumbers == null || ownedInputNumbers == null || ownedInputNumbers.length < minSize) {
            minSize = (int) (minSize * 1.2);
            if (inputNumbers != null) {
                ownedInputNumbers = Arrays.copyOf(inputNumbers, minSize);
            }
            else {
                ownedInputNumbers = new int[minSize];
            }
            inputNumbers = ownedInputNumbers;
        }
        else {
            System.arraycopy(inputNumbers, 0, ownedInputNumbers, 0, positionCount);
            inputNumbers = ownedInputNumbers;
        }
        return inputNumbers;
    }
    /*
    public int getBegin()
    {
        return begin;
    }

    public void setBegin(int begin)
    {
        this.begin = begin;
    }
    */
    public int getEnd()
    {
        if (truncationPosition != -1) {
            return positions[truncationPosition];
        }
        return end;
    }

    public int getNonTruncatedEnd()
    {
        return end;
    }

    public void setEnd(int end)
    {
        this.end = end;
    }

    public int getPositionCount()
    {
        if (truncationPosition != -1) {
            return truncationPosition;
        }
        return positionCount;
    }

    public int getTotalPositionCount()
    {
        return positionCount;
    }

    public int getTruncationPosition()
    {
        return truncationPosition;
    }

    public void setPositionCount(int positionCount)
    {
        this.positionCount = positionCount;
    }

    // Returns the first position after the argument position where
    // one can truncate a result column. For a top level column this
    // is the position itself. For a nested column, this is the
    // positioning corresponding to the first of row of the next top
    // level row.
    public int getNextTruncationPosition(int position)
    {
        return position;
    }

    public void setTruncationPosition(int position)
    {
        if (position >= positionCount || position <= 0) {
            throw new IllegalArgumentException();
        }
            truncationPosition = position;
    }

    public void clearTruncationPosition()
    {
        truncationPosition = -1;
    }

    public void setTruncationRow(int row)
    {
        if (row == -1) {
            clearTruncationPosition();
            return;
        }
        int pos = findPositionAtOrAbove(row);
        if (pos == positionCount) {
            clearTruncationPosition();
        }
        else {
            setTruncationPosition(pos);
        }
    }

    public int findPositionAtOrAbove(int row)
    {
        int pos = Arrays.binarySearch(positions, 0, positionCount, row);
        return pos < 0 ? -1 - pos : pos;
    }

    public ErrorSet getErrorSet()
    {
        return errorSet;
    }
    
    public void setErrorSet(ErrorSet errorSet)
    {
        this.errorSet = errorSet;
    }

    // Erases qulifying rows and corresponding input numbers below position.
    public void eraseBelowRow(int row)
    {
        if (positionCount == 0 || positions[positionCount - 1] < row) {
            positionCount = 0;
            return;
        }
        int surviving = findPositionAtOrAbove(row);
        if (surviving == positionCount) {
            positionCount = 0;
            return;
        }
        if (surviving == 0) {
            return;
        }
        positions = getMutablePositions(positionCount);
        inputNumbers = getMutableInputNumbers(positionCount);
        int lowestSurvivingInput = inputNumbers[surviving];
        for (int i = surviving; i < positionCount; i++) {
            positions[i - surviving] = positions[i];
            inputNumbers[i - surviving] = inputNumbers[i] - lowestSurvivingInput;
        }
        positionCount -= surviving;
    }

    public void copyFrom(QualifyingSet other)
    {
        positionCount = other.positionCount;
        end = other.end;
        truncationPosition = other.truncationPosition;
        if (ownedPositions != null && ownedPositions.length >= other.positionCount) {
            positions = ownedPositions;
            System.arraycopy(other.positions, 0, positions, 0, positionCount);
        }
        else {
            ownedPositions = Arrays.copyOf(other.positions, positionCount);
            positions = ownedPositions;
        }
        if (ownedInputNumbers != null && ownedInputNumbers.length >= positionCount) {
            inputNumbers = ownedInputNumbers;
            System.arraycopy(other.inputNumbers, 0, inputNumbers, 0, positionCount);
        }
        else {
            inputNumbers = Arrays.copyOf(other.inputNumbers, positionCount);
            ownedInputNumbers = inputNumbers;
        }
    }

    public void compactInputNumbers(int[] surviving, int numSurviving)
    {
        for (int i = 0; i < numSurviving; i++) {
            inputNumbers[i] = inputNumbers[surviving[i]];
        }
    }

    public void check()
    {
        for (int i = 0; i < positionCount; i++) {
            int pos = positions[i];
            if (pos >= end) {
                throw new IllegalArgumentException("QualifyingSet contains past end");
            }
            if (i > 0 && positions[i - 1] >= pos) {
                throw new IllegalArgumentException("QualifyingSet contains positions out of order");
            }
        }
    }
}
