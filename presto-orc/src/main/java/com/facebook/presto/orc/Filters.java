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

import com.facebook.presto.spi.SubfieldPath;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static com.facebook.presto.spi.block.ByteArrayUtils.memcmp;
import static com.google.common.base.Verify.verify;

public class Filters
{
    private Filters() {}

    public static class IsNull
            extends Filter
    {
        IsNull()
        {
            super(true);
        }

        @Override
        public boolean testNull()
        {
            return true;
        }
    }

    public static class BigintRange
            extends Filter
    {
        private final long lower;
        private final long upper;

        BigintRange(long lower, long upper, boolean nullAllowed)
        {
            super(nullAllowed);
            this.lower = lower;
            this.upper = upper;
        }

        @Override
        public boolean testLong(long value)
        {
            return value >= lower && value <= upper;
        }

        @Override
        int staticScore()
        {
            // Equality is better than range with both ends, which is better than a range with one end.
            if (upper == lower) {
                return 1;
            }
            return upper != Long.MAX_VALUE && lower != Long.MIN_VALUE ? 2 : 3;
        }

        public long getLower()
        {
            return lower;
        }

        public long getUpper()
        {
            return upper;
        }

        @Override
        public boolean isEquality()
        {
            return upper == lower;
        }
    }

    public static class DoubleRange
            extends Filter
    {
        private final double lower;
        private final boolean lowerUnbounded;
        private final boolean lowerExclusive;
        private final double upper;
        private final boolean upperUnbounded;
        private final boolean upperExclusive;

        DoubleRange(double lower, boolean lowerUnbounded, boolean lowerExclusive, double upper, boolean upperUnbounded, boolean upperExclusive, boolean nullAllowed)
        {
            super(nullAllowed);
            this.lower = lower;
            this.lowerUnbounded = lowerUnbounded;
            this.lowerExclusive = lowerExclusive;
            this.upper = upper;
            this.upperUnbounded = upperUnbounded;
            this.upperExclusive = upperExclusive;
        }

        @Override
        public boolean testDouble(double value)
        {
            if (!lowerUnbounded) {
                if (value < lower) {
                    return false;
                }
                if (lowerExclusive && lower == value) {
                    return false;
                }
            }
            if (!upperUnbounded) {
                if (value > upper) {
                    return false;
                }
                if (upperExclusive && value == upper) {
                    return false;
                }
            }
            return true;
        }

        @Override
        int staticScore()
        {
            // Equality is better than range with both ends, which is better than a range with one end.
            if (upper == lower) {
                return 1;
            }
            return upper != Long.MAX_VALUE && lower != Long.MIN_VALUE ? 2 : 3;
        }
    }

    public static class BytesRange
            extends Filter
    {
        private final byte[] lower;
        private final byte[] upper;
        private final boolean isEqual;
        private final boolean lowerInclusive;
        private final boolean upperInclusive;

        public BytesRange(byte[] lower, boolean lowerInclusive, byte[] upper, boolean upperInclusive, boolean nullAllowed)
        {
            super(nullAllowed);
            this.lower = lower;
            this.upper = upper;
            this.lowerInclusive = lowerInclusive;
            this.upperInclusive = upperInclusive;
            isEqual = upperInclusive && lowerInclusive && Arrays.equals(upper, lower);
        }

        @Override
        public boolean testBytes(byte[] buffer, int offset, int length)
        {
            if (isEqual) {
                if (length != lower.length) {
                    return false;
                }
                for (int i = 0; i < length; i++) {
                    if (buffer[i + offset] != lower[i]) {
                        return false;
                    }
                    return true;
                }
            }
            if (lower != null) {
                int lowerCmp = memcmp(buffer, offset, length, lower, 0, lower.length);
                if (lowerCmp < 0 || (!lowerInclusive && lowerCmp == 0)) {
                    return false;
                }
            }
            if (upper != null) {
                int upperCmp = memcmp(buffer, offset, length, upper, 0, upper.length);
                return upperCmp < 0 || (upperInclusive && upperCmp == 0);
            }
            return true;
        }

        @Override
        int staticScore()
        {
            // Equality is better than range with both ends, which is better than a range with one end.
            if (isEqual) {
                return 5;
            }
            return upper != null && lower != null ? 6 : 7;
        }

        public boolean isEquality()
        {
            return isEqual;
        }
    }

    public static class StructFilter
            extends Filter
    {
        private final HashMap<SubfieldPath.PathElement, Filter> filters = new HashMap();

        StructFilter()
        {
            super(false);
        }

        public Filter getMember(SubfieldPath.PathElement member)
        {
            return filters.get(member);
        }

        public void addMember(SubfieldPath.PathElement member, Filter filter)
        {
            filters.put(member, filter);
        }

        public HashMap<SubfieldPath.PathElement, Filter> getFilters()
        {
            return filters;
        }
    }

    public static class MultiRange
            extends Filter
    {
        Filter[] filters;
        long[] longLowerBounds;

        MultiRange(List<Filter> filters, boolean nullAllowed)
        {
            super(nullAllowed);
            this.filters = new Filter[filters.size()];
            for (int i = 0; i < this.filters.length; i++) {
                this.filters[i] = filters.get(i);
            }
            if (this.filters[0] instanceof BigintRange) {
                longLowerBounds = new long[this.filters.length];
                for (int i = 0; i < this.filters.length; i++) {
                    BigintRange range = (BigintRange) this.filters[i];
                    longLowerBounds[i] = range.getLower();
                    if (i > 0 && longLowerBounds[i] < ((BigintRange) this.filters[i - 1]).getUpper()) {
                        throw new IllegalArgumentException("Bigint filter range set must be in ascending order of lower bound and ranges must be disjoint");
                    }
                }
            }
        }

        @Override
        public boolean testLong(long value)
        {
            int i = Arrays.binarySearch(longLowerBounds, value);
            if (i >= 0) {
                return true;
            }
            int place = -1 - i;
            if (place == 0) {
                // Below first
                return false;
            }
            // When value did not hit a lower bound of a filter, test with the filter before the place where value would be inserted.
            return filters[place - 1].testLong(value);
        }

        @Override
        public boolean testDouble(double value)
        {
            for (Filter filter : filters) {
                if (filter.testDouble(value)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public boolean testBytes(byte[] buffer, int offset, int length)
        {
            for (Filter filter : filters) {
                if (filter.testBytes(buffer, offset, length)) {
                    return true;
                }
            }
            return false;
        }
    }

    public static class InTest
            extends Filter
    {
        static final long emptyMarker = 0xdeadbeefbadefeedL;
        static final long M = 0xc6a4a7935bd1e995L;
        private long[] longs;
        int size;
        private boolean containsEmptyMarker;

        public InTest(List<Filter> filters, boolean nullAllowed)
        {
            super(nullAllowed);
            size = Integer.highestOneBit((int) (filters.size() * 3));
            longs = new long[size];
            Arrays.fill(longs, emptyMarker);
            for (Filter filter : filters) {
                long value = ((BigintRange) filter).getLower();
                if (value == emptyMarker) {
                    containsEmptyMarker = true;
                }
                else {
                    int pos = (int) ((value * M) & (size - 1));
                    for (int i = pos; i < pos + size; i++) {
                        int idx = i & (size - 1);
                        if (longs[idx] == emptyMarker) {
                            longs[idx] = value;
                            break;
                        }
                    }
                }
            }
        }

        @Override
        public boolean testLong(long value)
        {
            if (containsEmptyMarker && value == emptyMarker) {
                return true;
            }
            int pos = (int) ((value * M) & (size - 1));
            for (int i = pos; i < pos + size; i++) {
                int idx = i & (size - 1);
                long l = longs[idx];
                if (l == emptyMarker) {
                    return false;
                }
                if (l == value) {
                    return true;
                }
            }
            return false;
        }
    }

    public static Filter createMultiRange(List<Filter> filters, boolean nullAllowed)
    {
        if (filters.get(0) instanceof BigintRange && filters.stream().allMatch(Filter::isEquality)) {
            return new InTest(filters, nullAllowed);
        }
        else {
            return new MultiRange(filters, nullAllowed);
        }
    }

    public static class PositionalFilter
            extends Filter
    {
        // The set of rows for which this specifies a Filter.
        private int[] filterRows;
        // The array/map number to which the filter refers to. All
        // filters with the same value here refer to the same
        // array/map. The first to fail will disqualify the rest of
        // the array/map.
        int[] filterContainer;
        int numFilterRows;
        //Filter for each row in filterRows. A null element means that the row has no filter.
        private Filter[] filters;
        // True if applying all filters in sequence.
        boolean allFilters;
        // Indices into filters. Each consecutive test consumes one. Used if allFilters is false.
        private int[] filterOrder;
        // Count of valid elements in filters/filterOrder.
        int numFilters;
        // Last used index in filters/filterOrder. -1 after initialization.
        int filterIdx;
        // Count of upcoming textXx calls to fail. Suppose an array of
        // 4 elements with a failed filter at first element. There
        // would be 3 elements to go that are in any case
        // disqualifuied, so the failing filter on the first element
        // would set this to 3.
        int failNext;

        public PositionalFilter()
        {
            super(false);
        }

        // Sets the filters to apply. filters corresponds pairwise to the rows in qualifyingSet.
        public void setFilters(QualifyingSet rows, Filter[] filters)
        {
            this.filterRows = rows.getPositions();
            this.filterContainer = rows.getInputNumbers();
            numFilterRows = rows.getPositionCount();
            this.filters = filters;
            filterIdx = -1;
        }

        @Override
        public boolean isDeterministic()
        {
            return false;
        }

        @Override
        public void setScanRows(int[] rows, int[] rowIndices, int numRows)
        {
            filterIdx = -1;
            failNext = 0;
            if (numRows == numFilterRows) {
                allFilters = true;
                numFilters = numFilterRows;
            }
            else {
                allFilters = false;
                numFilters = numRows;
                if (numFilters == 0) {
                    return;
                }
                if (filterOrder == null || filterOrder.length < numRows) {
                    filterOrder = new int[numRows];
                }
                int row = rowIndices != null ? rows[rowIndices[0]] : rows[0];
                int first = Arrays.binarySearch(filterRows, 0, numFilterRows, row);
                if (first < 0) {
                    throw new IllegalArgumentException("Filter row not in defined row set for PositionalFilter");
                }
                filterOrder[0] = first;
                for (int i = 1; i < numRows; i++) {
                    row = rowIndices != null ? rows[rowIndices[i]] : rows[i];
                    boolean found = false;
                    for (int j = filterOrder[i - 1] + 1; j < numFilterRows; j++) {
                        if (filterRows[j] == row) {
                            filterOrder[i] = j;
                            found = true;
                            break;
                        }
                        if (filterRows[j] > row) {
                            break;
                        }
                    }
                    if (!found) {
                        throw new IllegalArgumentException("Setting a filtered row that is not in the set of defined rows for PositionalFilter");
                    }
                }
            }
        }

        @Override
        public boolean testNull()
        {
            if (failNext > 0) {
                filterIdx++;
                failNext--;
                return false;
            }
            Filter filter = nextFilter();
            if (filter != null) {
                return processResult(filter.testNull());
            }
            return true;
        }

        @Override
        public boolean testLong(long value)
        {
            if (failNext > 0) {
                filterIdx++;
                failNext--;
                return false;
            }
            Filter filter = nextFilter();
            if (filter != null) {
                return processResult(filter.testLong(value));
            }
            return true;
        }

        @Override
        public boolean testBytes(byte[] value, int offset, int length)
        {
            if (failNext > 0) {
                filterIdx++;
                failNext--;
                return false;
            }
            Filter filter = nextFilter();
            if (filter != null) {
                return processResult(filter.testBytes(value, offset, length));
            }
            return true;
        }

        private Filter nextFilter()
        {
            filterIdx++;
            verify(filterIdx < numFilters);
            return allFilters ? filters[filterIdx] : filters[filterOrder[filterIdx]];
        }

        private boolean processResult(boolean result)
        {
            if (result == false) {
                // The remaining elements of the containing array/map will also be disqualified.
                if (allFilters) {
                    int nthArray = filterContainer[filterIdx];
                    for (int i = filterIdx + 1; i <numFilters; i++) {
                        if (filterContainer[i] != nthArray) {
                            break;
                        }
                        failNext++;
                    }
                }
                else {
                    int nthArray = filterContainer[filterOrder[filterIdx]];
                    for (int i = filterIdx + 1; i <numFilters; i++) {
                        if (filterContainer[filterOrder[i]] != nthArray) {
                            break;
                        }
                        failNext++;
                    }
                }
            }
            return result;
        }
    }
}
