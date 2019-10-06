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

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageSourceOptions;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;

import java.io.IOException;
import java.util.List;

public class FilterPushdownAdapter
{
    private final OrcPredicate predicate;
    private OrcRecordReader reader;

    public FilterPushdownAdapter(OrcPredicate predicate)
    {
        this.predicate = predicate;
    }

    public void pushdownFilterAndProjection(PageSourceOptions options, int[] splitColumnIndices, List<Type> splitColumnTypes, Block[] splitConstantBlocks, List<String> splitColumnNames)
    {
        reader = new OrcRecordReader(predicate, splitColumnIndices);
        reader.pushdownFilterAndProjection(options, splitColumnIndices, splitColumnTypes, splitConstantBlocks, splitColumnNames);
    }

    public Page postProcess(Page page)
            throws IOException
    {
        return reader.readBlockStreams(page);
    }
}
