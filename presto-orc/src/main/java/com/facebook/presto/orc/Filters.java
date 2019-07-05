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
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.facebook.presto.spi.block.ByteArrayUtils.hash;
import static com.facebook.presto.spi.block.ByteArrayUtils.memcmp;
import static com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic.compare;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class Filters
{
    private static final Filter ALWAYS_FALSE = new AlwaysFalse();
    private static final Filter IS_NULL = new IsNull();
    private static final Filter IS_NOT_NULL = new IsNotNull();

    private Filters() {}

    private static class AlwaysFalse
            extends Filter
    {
        public AlwaysFalse()
        {
            super(false);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this).toString();
        }
    }

    private static class IsNull
            extends Filter
    {
        public IsNull()
        {
            super(true);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this).toString();
        }
    }

    private static class IsNotNull
            extends Filter
    {
        public IsNotNull()
        {
            super(false);
        }

        @Override
        public boolean testLong(long value)
        {
            return true;
        }

        @Override
        public boolean testDouble(double value)
        {
            return true;
        }

        @Override
        public boolean testFloat(float value)
        {
            return true;
        }

        @Override
        public boolean testDecimal(long low, long high)
        {
            return true;
        }

        @Override
        public boolean testBoolean(boolean value)
        {
            return true;
        }

        @Override
        public boolean testBytes(byte[] buffer, int offset, int length)
        {
            return true;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this).toString();
        }
    }

    public static Filter alwaysFalse()
    {
        return ALWAYS_FALSE;
    }

    public static Filter isNull()
    {
        return IS_NULL;
    }

    public static Filter isNotNull()
    {
        return IS_NOT_NULL;
    }

    public static class BooleanValue
            extends Filter
    {
        private final boolean value;

        public BooleanValue(boolean value, boolean nullAllowed)
        {
            super(nullAllowed);
            this.value = value;
        }

        @Override
        public boolean testBoolean(boolean value)
        {
            return this.value == value;
        }

        @Override
        public int staticScore()
        {
            return 1;
        }

        @Override
        public boolean isEquality()
        {
            return true;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(value, nullAllowed);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            BooleanValue other = (BooleanValue) obj;
            return this.value == other.value &&
                    this.nullAllowed == other.nullAllowed;
        }
    }

    public static class BigintRange
            extends Filter
    {
        private final long lower;
        private final long upper;

        public BigintRange(long lower, long upper, boolean nullAllowed)
        {
            super(nullAllowed);
            checkArgument(lower <= upper, "lower must be <= upper");
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

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            BigintRange that = (BigintRange) o;
            return lower == that.lower &&
                    upper == that.upper &&
                    nullAllowed == that.nullAllowed;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(lower, upper, nullAllowed);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("lower", lower)
                    .add("upper", upper)
                    .add("nullAllowed", nullAllowed)
                    .toString();
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

    public static class FloatRange
            extends Filter
    {
        private final float lower;
        private final boolean lowerUnbounded;
        private final boolean lowerExclusive;
        private final float upper;
        private final boolean upperUnbounded;
        private final boolean upperExclusive;

        FloatRange(float lower, boolean lowerUnbounded, boolean lowerExclusive, float upper, boolean upperUnbounded, boolean upperExclusive, boolean nullAllowed)
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
        public boolean testFloat(float value)
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
            return !lowerUnbounded && !upperUnbounded ? 2 : 3;
        }
    }

    public static class LongDecimalRange
            extends Filter
    {
        private final long lowerLow;
        private final long lowerHigh;
        private final boolean lowerUnbounded;
        private final boolean lowerExclusive;
        private final long upperLow;
        private final long upperHigh;
        private final boolean upperUnbounded;
        private final boolean upperExclusive;

        public LongDecimalRange(long lowerLow, long lowerHigh, boolean lowerUnbounded, boolean lowerExclusive, long upperLow, long upperHigh, boolean upperUnbounded, boolean upperExclusive, boolean nullAllowed)
        {
            super(nullAllowed);
            this.lowerLow = lowerLow;
            this.lowerHigh = lowerHigh;
            this.lowerUnbounded = lowerUnbounded;
            this.lowerExclusive = lowerExclusive;
            this.upperLow = upperLow;
            this.upperHigh = upperHigh;
            this.upperUnbounded = upperUnbounded;
            this.upperExclusive = upperExclusive;
        }

        @Override
        public boolean testDecimal(long valueLow, long valueHigh)
        {
            if (!lowerUnbounded) {
                int result = compare(valueLow, valueHigh, lowerLow, lowerHigh);
                if (result < 0) {
                    return false;
                }
                if (lowerExclusive && result == 0) {
                    return false;
                }
            }
            if (!upperUnbounded) {
                int result = compare(valueLow, valueHigh, upperLow, upperHigh);
                if (result > 0) {
                    return false;
                }
                if (upperExclusive && result == 0) {
                    return false;
                }
            }
            return true;
        }

        @Override
        int staticScore()
        {
            // Equality is better than range with both ends, which is better than a range with one end.
            if (lowerLow == upperLow && lowerHigh == upperHigh) {
                return 1;
            }
            return !lowerUnbounded && !upperUnbounded ? 2 : 3;
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

        public byte[] getLower()
        {
            return lower;
        }

        @Override
        public boolean testBytes(byte[] buffer, int offset, int length)
        {
            if (isEqual) {
                if (length != lower.length) {
                    return false;
                }
                return memcmp(buffer, offset, length, upper, 0, upper.length) == 0;
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
        private final Map<SubfieldPath.PathElement, Filter> filters = new HashMap();
        private int ordinal;

        StructFilter()
        {
            super(false);
        }

        public void setNullAllowed(boolean nullAllowed)
        {
            this.nullAllowed = nullAllowed;
        }

        public Filter getMember(SubfieldPath.PathElement member)
        {
            return filters.get(member);
        }

        public void addMember(SubfieldPath.PathElement member, Filter filter)
        {
            verify(filters.get(member) == null, "Adding duplicate member filter" + member.toString());
            filters.put(member, filter);
            if (filter instanceof StructFilter) {
                // If nested struct filters, give each child a
                // distinct ordinal number, starting at 1 for
                // first. This is useful with positional filters where
                // different filters apply to different structs
                // depending on their position in a list/map.
                StructFilter childFilter = (StructFilter) filter;
                childFilter.ordinal = filters.size();
            }
        }

        public Map<SubfieldPath.PathElement, Filter> getFilters()
        {
            return filters;
        }

        public int getOrdinal()
        {
            return ordinal;
        }

        public void setOrdinal(int ordinal)
        {
            this.ordinal = ordinal;
        }

        public boolean isOnlyIsNulls()
        {
            for (Map.Entry<SubfieldPath.PathElement, Filter> entry : filters.entrySet()) {
                if (entry.getValue() instanceof StructFilter && !((StructFilter) entry.getValue()).isOnlyIsNulls()) {
                    return false;
                }
                if (entry.getValue() != isNull()) {
                    return false;
                }
            }
            return true;
        }
    }

    public static class MultiRange
            extends Filter
    {
        private final Filter[] filters;
        private final long[] longLowerBounds;

        MultiRange(List<Filter> filters, boolean nullAllowed)
        {
            super(nullAllowed);
            requireNonNull(filters, "filters is null");
            checkArgument(filters.size() > 0, "filters is empty");

            this.filters = filters.toArray(new Filter[0]);
            if (filters.get(0) instanceof BigintRange) {
                this.longLowerBounds = filters.stream()
                        .mapToLong(filter -> ((BigintRange) filter).getLower())
                        .toArray();

                long[] upperBounds = filters.stream()
                        .mapToLong(filter -> ((BigintRange) filter).getUpper())
                        .toArray();

                for (int i = 1; i < longLowerBounds.length; i++) {
                    checkArgument(longLowerBounds[i] >= upperBounds[i - 1], "bigint ranges must not overlap");
                }
            }
            else {
                this.longLowerBounds = null;
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
        public boolean testFloat(float value)
        {
            for (Filter filter : filters) {
                if (filter.testFloat(value)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public boolean testDecimal(long valueLow, long valueHigh)
        {
            for (Filter filter : filters) {
                if (filter.testDecimal(valueLow, valueHigh)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public boolean testBoolean(boolean value)
        {
            for (Filter filter : filters) {
                if (filter.testBoolean(value)) {
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

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            MultiRange that = (MultiRange) o;
            return Arrays.equals(filters, that.filters) &&
                    Arrays.equals(longLowerBounds, that.longLowerBounds) &&
                    nullAllowed == that.nullAllowed;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(filters, longLowerBounds, nullAllowed);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("filters", filters)
                    .add("nullAllowed", nullAllowed)
                    .toString();
        }
    }

    public static class BingintValues
            extends Filter
    {
        private static final long EMPTY_MARKER = 0xdeadbeefbadefeedL;
        private static final long M = 0xc6a4a7935bd1e995L;

        private final long[] originalValues;
        private final long[] longs;
        private final int size;
        private boolean containsEmptyMarker;

        public BingintValues(long[] values, boolean nullAllowed)
        {
            super(nullAllowed);
            originalValues = values;
            size = Integer.highestOneBit(values.length * 3);
            longs = new long[size];
            Arrays.fill(longs, EMPTY_MARKER);
            for (long value : values) {
                if (value == EMPTY_MARKER) {
                    containsEmptyMarker = true;
                }
                else {
                    int pos = (int) ((value * M) & (size - 1));
                    for (int i = pos; i < pos + size; i++) {
                        int idx = i & (size - 1);
                        if (longs[idx] == EMPTY_MARKER) {
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
            if (containsEmptyMarker && value == EMPTY_MARKER) {
                return true;
            }
            int pos = (int) ((value * M) & (size - 1));
            for (int i = pos; i < pos + size; i++) {
                int idx = i & (size - 1);
                long l = longs[idx];
                if (l == EMPTY_MARKER) {
                    return false;
                }
                if (l == value) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            BingintValues that = (BingintValues) o;
            return size == that.size &&
                    containsEmptyMarker == that.containsEmptyMarker &&
                    Arrays.equals(longs, that.longs) &&
                    nullAllowed == that.nullAllowed;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(size, containsEmptyMarker, longs, nullAllowed);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("values", originalValues)
                    .add("nullAllowed", nullAllowed)
                    .toString();
        }
    }

    public static class BytesValues
            extends Filter
    {
        private static final long M = 0xc6a4a7935bd1e995L;

        private final byte[][] originalValues;
        private final byte[][] values;
        private final int size;
        long[] bloom;
        int bloomSize;

        public BytesValues(byte[][] valueSet, boolean nullAllowed)
        {
            super(nullAllowed);
            originalValues = valueSet;
            size = Integer.highestOneBit(valueSet.length * 5);
            values = new byte[size][];
            bloomSize = (size + 1) / 8;
            bloom = new long[bloomSize];
            for (byte[] value : valueSet) {
                long hashCode = hash(value, 0, value.length);
                addBloom(hashCode);
                int pos = (int) (hashCode & (size - 1));
                for (int i = pos; i < pos + size; i++) {
                    int idx = i & (size - 1);
                    if (values[idx] == null) {
                        values[idx] = value;
                            break;
                        }
                    if (memcmp(value, 0, value.length, values[idx], 0, values[idx].length) == 0) {
                        break;
                    }
                }
            }
        }

        @Override
        public boolean testBytes(byte[] value, int offset, int length)
        {
            long hashCode = hash(value, offset, length);
            if (!testBloom(hashCode)) {
                return false;
            }
            int pos = (int) (hashCode & (size - 1));
            for (int i = pos; i < pos + size; i++) {
                int idx = i & (size - 1);
                byte[] entry = values[idx];
                if (entry == null) {
                    return false;
                }
                if (memcmp(value, offset, length, entry, 0, entry.length) == 0) {
                    return true;
                }
            }
            return false;
        }

        private static long bloomMask(long hashCode)
        {
            return (1L << ((hashCode >> 22) & 63)) |  (1L << ((hashCode >> 30) & 63)) | (1L << ((hashCode >> 38) & 63));
        }

        private int bloomIndex(long hashCode)
        {
            return (int) ((hashCode >> 47) & (bloomSize - 1));
        }

        private void addBloom(long hashCode)
        {
            bloom[bloomIndex(hashCode)] |= bloomMask(hashCode);
        }

        private boolean testBloom(long hashCode)
        {
            long mask = bloomMask(hashCode);
            int index = bloomIndex(hashCode);
            return mask == (bloom[index] & mask);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            BytesValues that = (BytesValues) o;
            return size == that.size &&
                    Arrays.equals(values, that.values) &&
                    nullAllowed == that.nullAllowed;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(size, originalValues, nullAllowed);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("values", originalValues)
                    .add("nullAllowed", nullAllowed)
                    .toString();
        }
    }

    public static Filter createMultiRange(List<Filter> filters, boolean nullAllowed)
    {
        requireNonNull(filters, "filters is null");
        checkArgument(filters.size() > 0, "filters is empty");
        if (filters.get(0) instanceof BigintRange && filters.stream().allMatch(Filter::isEquality)) {
            return new BingintValues(filters.stream().mapToLong(filter -> ((BigintRange) filter).getLower()).toArray(), nullAllowed);
        }
        else         if (filters.get(0) instanceof BytesRange && filters.stream().allMatch(Filter::isEquality)) {
            return new BytesValues(filters.stream().map(filter -> ((BytesRange) filter).getLower()).toArray(c -> new byte[c][]), nullAllowed);
        }

        else {
            return new MultiRange(filters, nullAllowed);
        }
    }

    public static class PositionalFilter
            extends Filter
    {
        // The set of row numbers for which this specifies a Filter.
        private int[] positions;
        // The position of the array/map that corresponds to the
        // matching element in positions.  filters with the same value
        // here refer to the same array/map. The first to fail will
        // disqualify the rest of the array/map.
        private int[] inputNumbers;
        // Number of valid entries in positions/inputNumbers.
        private int numPositions;
        //Filter for each position. A null element means that the position has no filter.
        private Filter[] filters;

        // Subset of positions for which the filter will be
        // evaluated. This is an ascending set of indices into
        // positions/filters.
        private int[] selectedPositionIndexes;
        // Count of valid elements in filters/selectedPositionIndexes.
        private int numSelectedPositions;
        // True if applying all filters in sequence. selectedPositionIndexes is not used if this is true.
        private boolean scanAllPositions;

        // Last used index in filters/selectedPositionIndexes. -1 after initialization.
        private int filterIndex;
        // Count of upcoming testXx calls to fail. Suppose an array of
        // 4 elements with a failed filter at the first element. There
        // would be 3 elements to go that are in any case
        // disqualifuied, so the failing filter on the first element
        // would set this to 3.
        private int numNextPositionsToFail;

        // The StructFilter from which the positional filters are derived.
        private StructFilter parent;

        // The set of StructFilters that may occur in filters. Empty if this filters a scalar column.
        private List<Filter> distinctChildFilters = new ArrayList();

        public PositionalFilter(StructFilter parent)
        {
            super(false);
            this.parent = requireNonNull(parent, "parent is null");
        }

        // Sets the filters to apply. elementFilters corresponds pairwise to the rows in qualifyingSet.
        public void setFilters(QualifyingSet rows, Filter[] elementFilters)
        {
            requireNonNull(rows, "rows is null");
            requireNonNull(elementFilters, "elementFilters is null");
            checkArgument(elementFilters.length >= rows.getPositionCount(), "Not enough filters");
            positions = rows.getPositions();
            inputNumbers = rows.getInputNumbers();
            numPositions = rows.getPositionCount();
            numSelectedPositions = rows.getPositionCount();
            filters = elementFilters;
            filterIndex = -1;
        }

        @Override
        public boolean isDeterministic()
        {
            return false;
        }

        @Override
        public void setScanRows(int[] rows, int[] rowIndices, int numRows)
        {
            checkArgument(numRows <= rows.length);
            checkArgument(numRows <= numPositions);
            filterIndex = -1;
            numNextPositionsToFail = 0;
            if (numRows == numPositions) {
                scanAllPositions = true;
                numSelectedPositions = numPositions;
            }
            else {
                scanAllPositions = false;
                numSelectedPositions = numRows;
                if (numSelectedPositions == 0) {
                    return;
                }
                if (selectedPositionIndexes == null || selectedPositionIndexes.length < numRows) {
                    selectedPositionIndexes = new int[numRows];
                }
                int row = rowIndices != null ? rows[rowIndices[0]] : rows[0];
                int first = Arrays.binarySearch(positions, 0, numPositions, row);
                verify(first >= 0, "Filter row not in defined row set for PositionalFilter");
                selectedPositionIndexes[0] = first;
                for (int i = 1; i < numRows; i++) {
                    row = rowIndices != null ? rows[rowIndices[i]] : rows[i];
                    boolean found = false;
                    for (int j = selectedPositionIndexes[i - 1] + 1; j < numPositions; j++) {
                        if (positions[j] == row) {
                            selectedPositionIndexes[i] = j;
                            found = true;
                            break;
                        }
                        if (positions[j] > row) {
                            break;
                        }
                    }
                    verify(found, "Row not found in PositionalFilter " + row);
                }
            }
        }

        public void addChild(Filter child)
        {
            if (child instanceof StructFilter && distinctChildFilters.indexOf(child) < 0) {
                distinctChildFilters.add(child);
                ((StructFilter) child).setOrdinal(distinctChildFilters.size());
            }
        }

        List<Filter> getDistinctChildren()
        {
            return distinctChildFilters;
        }

        @Override
        public boolean testNull()
        {
            if (numNextPositionsToFail > 0) {
                filterIndex++;
                numNextPositionsToFail--;
                return false;
            }
            Filter filter = nextFilter();
            if (filter != null) {
                return processResult(filter.testNull());
            }
            return true;
        }

        @Override
        public boolean testNotNull()
        {
            if (numNextPositionsToFail > 0) {
                filterIndex++;
                numNextPositionsToFail--;
                return false;
            }
            Filter filter = nextFilter();
            if (filter != null) {
                return processResult(filter.testNotNull());
            }
            return true;
        }

        @Override
        public boolean testLong(long value)
        {
            if (numNextPositionsToFail > 0) {
                filterIndex++;
                numNextPositionsToFail--;
                return false;
            }
            Filter filter = nextFilter();
            if (filter != null) {
                return processResult(filter.testLong(value));
            }
            return true;
        }

        @Override
        public boolean testDouble(double value)
        {
            if (numNextPositionsToFail > 0) {
                filterIndex++;
                numNextPositionsToFail--;
                return false;
            }
            Filter filter = nextFilter();
            if (filter != null) {
                return processResult(filter.testDouble(value));
            }
            return true;
        }

        @Override
        public boolean testFloat(float value)
        {
            if (numNextPositionsToFail > 0) {
                filterIndex++;
                numNextPositionsToFail--;
                return false;
            }
            Filter filter = nextFilter();
            if (filter != null) {
                return processResult(filter.testFloat(value));
            }
            return true;
        }

        @Override
        public boolean testDecimal(long low, long high)
        {
            if (numNextPositionsToFail > 0) {
                filterIndex++;
                numNextPositionsToFail--;
                return false;
            }
            Filter filter = nextFilter();
            if (filter != null) {
                return processResult(filter.testDecimal(low, high));
            }
            return true;
        }

        @Override
        public boolean testBoolean(boolean value)
        {
            if (numNextPositionsToFail > 0) {
                filterIndex++;
                numNextPositionsToFail--;
                return false;
            }
            Filter filter = nextFilter();
            if (filter != null) {
                return processResult(filter.testBoolean(value));
            }
            return true;
        }

        @Override
        public boolean testBytes(byte[] value, int offset, int length)
        {
            if (numNextPositionsToFail > 0) {
                filterIndex++;
                numNextPositionsToFail--;
                return false;
            }
            Filter filter = nextFilter();
            if (filter != null) {
                return processResult(filter.testBytes(value, offset, length));
            }
            return true;
        }

        @Override
        public Filter nextFilter()
        {
            filterIndex++;
            verify(filterIndex < numSelectedPositions);
            return scanAllPositions ? filters[filterIndex] : filters[selectedPositionIndexes[filterIndex]];
        }

        private boolean processResult(boolean result)
        {
            if (result == false) {
                // The remaining elements of the containing array/map will also be disqualified.
                if (scanAllPositions) {
                    int inputNumber = inputNumbers[filterIndex];
                    for (int i = filterIndex + 1; i < numSelectedPositions; i++) {
                        if (inputNumbers[i] != inputNumber) {
                            break;
                        }
                        numNextPositionsToFail++;
                    }
                }
                else {
                    int inputNumber = inputNumbers[selectedPositionIndexes[filterIndex]];
                    for (int i = filterIndex + 1; i < numSelectedPositions; i++) {
                        if (inputNumbers[selectedPositionIndexes[i]] != inputNumber) {
                            break;
                        }
                        numNextPositionsToFail++;
                    }
                }
            }
            return result;
        }

        public StructFilter getParent()
        {
            return parent;
        }
    }

    public static List<Filter> getDistinctPositionFilters(Filter filter)
    {
        if (filter == null) {
            return ImmutableList.of();
        }
        if (filter instanceof PositionalFilter) {
            List<Filter> distinctChildren = ((PositionalFilter) filter).getDistinctChildren();
            if (distinctChildren.size() > 0) {
                return distinctChildren;
            }
            return ImmutableList.copyOf(((PositionalFilter) filter).getParent().getFilters().values());
        }
        return ImmutableList.of(filter);
    }

    public static boolean isAlwaysFalse(Filter filter)
    {
        if (filter == alwaysFalse()) {
            return true;
        }
        else if (filter instanceof StructFilter) {
            for (Map.Entry<SubfieldPath.PathElement, Filter> entry : ((StructFilter) filter).getFilters().entrySet()) {
                if (isAlwaysFalse(entry.getValue())) {
                    return true;
                }
            }
        }
        return false;
    }

    public static class CoercingFilter
            extends Filter
    {
        private final Filter filter;
        private final long minValue;
        private final long maxValue;
        private final boolean isToString;

        public CoercingFilter(Filter filter, long minValue, long maxValue, boolean isToString, boolean nullAllowed) {
            super(nullAllowed);
            this.filter = filter;
            this.minValue = minValue;
            this.maxValue = maxValue;
            this.isToString = isToString;
        }

        @Override
        public boolean isDeterministic()
        {
            return filter.isDeterministic();
        }

        @Override
        public boolean testNull()
        {
            return filter.testNull();
        }

        @Override
        public boolean testNotNull()
        {
            return filter.testNotNull();
        }

        @Override
        public boolean testLong(long value)
        {
            if (isToString) {
                byte[] bytes = String.valueOf(value).getBytes(UTF_8);
                return filter.testBytes(bytes, 0, bytes.length);
            }
            if (value < minValue || value > maxValue) {
                return testNull();
            }
            return filter.testLong(value);
        }

        @Override
        public boolean testBytes(byte[] bytes, int offset, int length)
        {
            try {
                long value = Long.parseLong(new String(bytes, offset, length));
                if (minValue <= value && value <= maxValue) {
                    return filter.testLong(value);
                }
                else {
                    return filter.testNull();
                }
            }
            catch (NumberFormatException e) {
                return filter.testNull();
            }
        }

        @Override
        public boolean testFloat(float value)
        {
            return filter.testDouble((double) value);
        }

        @Override
        public boolean testDouble(double value)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean testBoolean(boolean value)
        {
            throw new UnsupportedOperationException();
        }

                @Override
                public boolean testDecimal(long low, long high)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                .add("Coercing filter", filter.toString())
                .toString();
        }
    }
}
