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

import com.facebook.presto.spi.trace.Trace;
import com.facebook.presto.util.Mergeable;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ScanInfo
    implements Mergeable<ScanInfo>, OperatorInfo
{
    private final String label;
    private final List<FilterInfo> filters;

    @JsonCreator
    public ScanInfo(
                    @JsonProperty("label") String label,
                    @JsonProperty("filters") List<FilterInfo> filters)
    {
        this.label = label;
        this.filters = filters;
        if (Trace.isTrace("scaninfo")) {
            Trace.trace("newScanInfo " + Trace.stackTrace(10));
        }
    }

    @Override
    public boolean isFinal()
    {
        return true;
    }

    @JsonProperty
    public String getLabel()
    {
        return label;
    }

    @JsonProperty
    public List<FilterInfo> getFilters()
    {
        return filters;
    }

    @Override
    public ScanInfo mergeWith(ScanInfo other)
    {
        Map<String, FilterInfo> result = new HashMap();
        for (FilterInfo filter : filters) {
            result.put(filter.getLabel(), filter);
        }
        for (FilterInfo filter : other.filters) {
            FilterInfo mergeWith = result.get(filter.getLabel());
            if (mergeWith != null) {
                result.put(filter.getLabel(), new FilterInfo(filter.getLabel(), mergeWith.getNIn() + filter.getNIn(), mergeWith.getNOut() + filter.getNOut()));
            }
            else {
                result.put(filter.getLabel(), filter);
            }
        }
        if (Trace.isTrace("scaninfo")) {
            Trace.trace("mergeScanInfo " + Trace.stackTrace(10));
        }
        return new ScanInfo(label, ImmutableList.copyOf(result.values()));
    }

    public static class FilterInfo
    {
        private String label;
        private long nIn;
        private long nOut;

        @JsonCreator
        public FilterInfo(@JsonProperty("label") String label,
                          @JsonProperty("nIn") long nIn,
                          @JsonProperty("nOut") long nOut)
        {
            this.label = label;
            this.nIn = nIn;
            this.nOut = nOut;
        }

        @JsonProperty
        public String getLabel()
        {
            return label;
        }

        @JsonProperty
        public long getNIn()
        {
            return nIn;
        }

        @JsonProperty
        public long getNOut()
        {
            return nOut;
        }
    }
}
