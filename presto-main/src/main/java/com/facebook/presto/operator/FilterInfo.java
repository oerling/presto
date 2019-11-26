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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class FilterInfo
{
    private String label;
    private long nIn;
    private long nOut;

    @JsonCreator
    public FilterInfo(@JsonProperty("label") String label,
                      @JsonProperty("counts") String counts,
                      @JsonProperty("nIn") long nIn,
                      @JsonProperty("nOut") long nOut)
    {
        this.label = label;
        try {
            String[] countsArray = counts.split("/");
            this.nIn = Long.parseLong(countsArray[2]);
            this.nOut = Long.parseLong(countsArray[1]);
        }
        catch (Exception e) {
            // Silently ignore.
            System.out.println("Bad counts in FilterInfo: " + counts);
        }
            if (this.nIn > 0) {
            System.out.println("***");
        }
    }

    public FilterInfo(String label,
                      long nIn,
                      long nOut)
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
    public String getCounts()
    {
        return "/" + Long.valueOf(nOut).toString() + "/" + Long.valueOf(nIn).toString();
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

    @Override
    public String toString()
    {
        return "<Filter " + label + " " + nOut + "/" + nIn + ">";
    }
}
