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
            Trace.trace("newScanInfo " + toString() + Trace.stackTrace(30));
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
        ScanInfo merged = new ScanInfo(label, ImmutableList.copyOf(result.values()));
        if (Trace.isTrace("scaninfo")) {
            Trace.trace("mergeScanInfo " + merged.toString() + Trace.stackTrace(10));
        }
        return merged;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append(label);
        for (FilterInfo filter : filters) {
            builder.append(filter.toString());
        }
        return builder.toString();
    }
}
