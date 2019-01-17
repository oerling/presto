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

import java.util.Arrays;

public class PageSourceOptions
{
    private final boolean reusePages;
    private final int[] internalChannels;
    private final int[] outputChannels;
    private final FilterFunction[] filterFunctions;
    private final boolean reorderFilters;
    private final int targetBytes;
    private final int ariaFlags;

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
            nIn /= 2;
            nOut /= 2;
            time /= 2;
        }
    }
    
    public static class FilterFunction
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

        
        /* Sets outputRows to be the list of positions on page for
         * which the filter is true. Returns the number of positions
         * written to outputRows. outputRows is expected to have at
         * least page.getPositionCount() elements. If errorSet is non
         * null, exceptions are caught and returned in errorSet. These
         * correspond pairwise to the row numbers in rows. A row that
         * produces an error is considered as included in the
         * output. */
        public int filter(Page page, int[] outputRows, ErrorSet errorSet)
        {
            return 0;
        }

        public int[][] getChannelRowNumberMaps()
        {
            return channelRowNumberMaps;
        }
    }

    public static class ErrorSet
    {
        int positionCount;
        private Throwable[] errors;

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
            positionCount = 0;
            if (errors != null) {
                // Drop the references, errors may be large.
                Arrays.fill(errors, null);
            }
        }

        public void erase(int end)
        {
            if (positionCount <= end) {
                clear();
            }
            System.arraycopy(errors, end, errors, 0, positionCount - end);
            positionCount -= end;
        }
        
        public void addError(int position, int maxPosition, Throwable error)
        {
            if (errors == null) {
                errors = new Throwable[maxPosition];
            }
            else if (errors.length < maxPosition) {
                errors = Arrays.copyOf(errors, maxPosition);
            }
            errors[position] = error;
            if (position >= positionCount) {
                positionCount = position + 1;
            }
        }

        public Throwable[] getErrors()
        {
            return errors;
        }

        public void setErrors(Throwable[] errors, int positionCount)
        {
            if (positionCount > errors.length) {
                throw new IllegalArgumentException("positionCount is larger than the errors array");
            }
            this.positionCount = positionCount;
            this.errors = errors;
        }

        public Throwable getFirstError(int numPositions)
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
                             boolean reorderFilters,
                             int targetBytes,
                             int ariaFlags)
    {
        this.internalChannels = internalChannels;
        this.outputChannels = outputChannels;
        this.reusePages = reusePages;
        this.filterFunctions = filterFunctions;
        this.reorderFilters = reorderFilters;
        this.targetBytes = targetBytes;
        this.ariaFlags = ariaFlags;
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

    public boolean getReorderFilters()
    {
        return reorderFilters;
    }

    public int getTargetBytes()
    {
        return targetBytes;
    }

    public int getAriaFlags()
    {
        return ariaFlags;
    }
    }
