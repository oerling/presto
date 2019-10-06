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
package com.facebook.presto.spi;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class PageSourceOptions
{
    private final boolean reusePages;
    private final int[] internalChannels;
    private final int[] outputChannels;
    private final FilterFunction[] filterFunctions;
    private final int targetBytes;
    private final ScanInfo scanInfo;
    // The ordinal of the column in the table. Can be negative for a
    // column not physically in the table, e.g. prefilled. Corresponds
    // pairwise to internalChannels, prefilledValues, types and coercers.
    private final int[] columnIndices;
    private final Object[] prefilledValues;
    private final Type[] types;
    private final Function<Block, Block>[] coercers;

    public abstract static class AdaptationStats
    {
    }

    public static class ScanInfo
    {
        private AdaptationStats stats;
        private long numScannedRows;
        private List<String> filterLabels;
        private List<FilterStats> filterStats;

        public AdaptationStats getStats()
        {
            return stats;
        }

        public void setStats(AdaptationStats stats)
        {
            this.stats = stats;
        }

        public void incrementScannedRows(long numRows)
        {
            numScannedRows += numRows;
        }

        public long getNumScannedRows()
        {
            return numScannedRows;
        }

        public List<FilterStats> getFilterStats()
        {
            return filterStats;
        }

        public List<String> getFilterLabels()
        {
            return filterLabels;
        }

        public void setFilterStats(List<String> labels, List<FilterStats> filterStats)
        {
            this.filterLabels = labels;
            this.filterStats = filterStats;
        }
    }

    public static class FilterStats
    {
        protected long nIn;
        protected long nOut;
        protected long time;

        public void updateStats(int nIn, int nOut, long time)
        {
            this.nIn += nIn;
            this.nOut += nOut;
            this.time += time;
        }

        public double getTimePerDroppedValue()
        {
            return (double) time / (1 + nIn - nOut);
        }

        public double getSelectivity()
        {
            if (nIn == 0) {
                return 1;
            }
            return (double) nOut / (double) nIn;
        }

        public void decayStats()
        {
            // Do not decay nOut down to 0 because this would make finite selectivity infinite.
            if (nOut > 1) {
                nIn /= 2;
                nOut /= 2;
                time /= 2;
            } 
        }
        public long getNIn()
        {
            return nIn;
        }

        public long getNOut()
        {
            return nOut;
        }
    }

    public abstract static class FilterFunction
            extends FilterStats
    {
        protected final int[] inputChannels;
        protected int initialCost = 1;
        private int[][] channelRowNumberMaps;

        public FilterFunction(int[] inputChannels, int initialCost)
        {
            this.inputChannels = inputChannels;
            this.channelRowNumberMaps = new int[inputChannels.length][];
            this.initialCost = initialCost;
        }

        public int[] getInputChannels()
        {
            return inputChannels;
        }

        public abstract boolean isDeterministic();

        /* Sets outputRows to be the list of positions on page for
         * which the filter is true. Returns the number of positions
         * written to outputRows. outputRows is expected to have at
         * least page.getPositionCount() elements. If errorSet is non
         * null, exceptions are caught and returned in errorSet. These
         * correspond pairwise to the row numbers in rows. A row that
         * produces an error is considered as included in the
         * output. */
        public abstract int filter(Page page, int[] outputRows, ErrorSet errorSet);

        public int[][] getChannelRowNumberMaps()
        {
            return channelRowNumberMaps;
        }
    }

    public static class ErrorSet
    {
        private int positionCount;
        private RuntimeException[] errors;

        public boolean isEmpty()
        {
            for (int i = 0; i < positionCount; i++) {
                if (errors[i] != null) {
                    return false;
                }
            }
            return true;
        }

        public int getPositionCount()
        {
            return positionCount;
        }

        public void clear()
        {
            if (errors != null) {
                // Drop the references, errors may be large.
                Arrays.fill(errors, 0, positionCount, null);
            }
            positionCount = 0;
        }

        public void erase(int end)
        {
            if (positionCount <= end) {
                clear();
                return;
            }
            System.arraycopy(errors, end, errors, 0, positionCount - end);
            Arrays.fill(errors, end, positionCount, null);
            positionCount -= end;
        }

        public void addError(int position, int maxPosition, RuntimeException error)
        {
            if (errors == null) {
                errors = new RuntimeException[maxPosition];
            }
            else if (errors.length < maxPosition) {
                errors = Arrays.copyOf(errors, maxPosition);
            }
            errors[position] = error;
            if (position >= positionCount) {
                for (int i = positionCount; i < position; i++) {
                    errors[i] = null;
                }
                positionCount = position + 1;
            }
        }

        public RuntimeException[] getErrors()
        {
            return errors;
        }

        public void setErrors(RuntimeException[] errors, int positionCount)
        {
            if (positionCount > errors.length) {
                throw new IllegalArgumentException("positionCount is larger than the errors array");
            }
            this.positionCount = positionCount;
            this.errors = errors;
        }

        public RuntimeException getFirstError(int numPositions)
        {
            int end = Math.min(positionCount, numPositions);
            for (int i = 0; i < end; i++) {
                if (errors[i] != null) {
                    return errors[i];
                }
            }
            return null;
        }
    }

    public PageSourceOptions(int[] internalChannels,
                             int[] outputChannels,
                             boolean reusePages,
                             FilterFunction[] filterFunctions,
                             int targetBytes,
                             ScanInfo scanInfo,
                             int[] columnIndices,
                             Object[] prefilledValues,
                             Type[] types,
                             Function<Block, Block>[] coercers)
    {
        this.internalChannels = requireNonNull(internalChannels, "internalChannels is null");
        this.outputChannels = requireNonNull(outputChannels, "outputChannels is null");
        this.reusePages = reusePages;
        this.filterFunctions = requireNonNull(filterFunctions, "filterFunctions is null");
        this.targetBytes = targetBytes;
        this.scanInfo = requireNonNull(scanInfo, "scanInfo is null");
        this.columnIndices = columnIndices;
        this.prefilledValues = prefilledValues;
        this.types = types;
        this.coercers = coercers;
    }

    public PageSourceOptions(int[] internalChannels,
                             int[] outputChannels,
                             boolean reusePages,
                             FilterFunction[] filterFunctions,
                                 int targetBytes)
    {
        this(internalChannels, outputChannels, reusePages, filterFunctions, targetBytes, new ScanInfo(), null, null, null, null);
    }

    public int[] getInternalChannels()
    {
        return internalChannels;
    }

    public int[] getOutputChannels()
    {
        return outputChannels;
    }

    public boolean getReusePages()
    {
        return reusePages;
    }

    public FilterFunction[] getFilterFunctions()
    {
        return filterFunctions;
    }

    public int getTargetBytes()
    {
        return targetBytes;
    }
    public ScanInfo getScanInfo()
    {
        return scanInfo;
    }

    public int[] getColumnIndices()
    {
        return columnIndices;
    }

    public Object[] getPrefilledValues()
    {
        return prefilledValues;
    }

    public Type[] getTypes()
    {
        return types;
    }

    public Function<Block, Block>[] getCoercers()
    {
        return coercers;
    }
}
