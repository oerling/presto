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
import com.facebook.presto.spi.PageSourceOptions.FilterFunction;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;

import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.orc.OrcReader.MAX_BATCH_SIZE;
import static com.facebook.presto.orc.OrcTester.HIVE_STORAGE_TIME_ZONE;
import static com.google.common.io.Resources.getResource;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.testng.Assert.assertEquals;

public class TestDwrfDictionaries
{
    @Test
    public void testSlice()
            throws Exception
    {
        Block block1 = readFile("test_dictionaries/string-dictionary.orc", VarcharType.VARCHAR);
        Block block2 = readFile("test_dictionaries/string-row-group-dictionary.dwrf", VarcharType.VARCHAR);
        compare(VarcharType.VARCHAR, block1, block2);
    }

    private void compare(Type type, Block block1, Block block2)
    {
        assertEquals(block1.getPositionCount(), block2.getPositionCount());
        for (int i = 0; i < block1.getPositionCount(); i++) {
            boolean null1 = block1.isNull(i);
            boolean null2 = block2.isNull(i);
            assertEquals(null1, null2);
            if (!null1) {
                assertEquals(type.compareTo(block1, i, block2, i), 0);
            }
        }
    }

    Block readFile(String testOrcFileName, Type type)
            throws IOException
    {
        OrcEncoding encoding = testOrcFileName.contains(".orc") ? OrcEncoding.ORC : OrcEncoding.DWRF;
        File file = new File(getResource(testOrcFileName).getFile());
        try (OrcRecordReader recordReader = createCustomOrcRecordReader(file, encoding, type, MAX_BATCH_SIZE)) {
            PageSourceOptions options = new PageSourceOptions(
                        new int[] {0},
                        new int[] {0},
                        true,
                        new FilterFunction[0],
                        true,
                        512 * 1024,
                        127);
            recordReader.pushdownFilterAndProjection(options, new int[] {0}, ImmutableList.of(type));

            assertEquals(recordReader.getReaderPosition(), 0);
            assertEquals(recordReader.getFilePosition(), 0);
            BlockBuilder builder = type.createBlockBuilder(null, 61000);
            while (true) {
                Page page = recordReader.getNextPage();
                if (page == null) {
                    break;
                }
                Block block = page.getBlock(0);
                int count = block.getPositionCount();
                for (int i = 0; i < count; i++) {
                    block.writePositionTo(i, builder);
                }
            }
            return builder.build();
        }
    }

    static OrcRecordReader createCustomOrcRecordReader(File file, OrcEncoding orcEncoding, Type type, int initialBatchSize)
            throws IOException
    {
        OrcDataSource orcDataSource = new FileOrcDataSource(file, new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), true);
        OrcReader orcReader = new OrcReader(orcDataSource, orcEncoding, new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE));

        return orcReader.createRecordReader(ImmutableMap.of(0, type), new TestingOrcPredicate.EmptyOrcPredicate(), HIVE_STORAGE_TIME_ZONE, newSimpleAggregatedMemoryContext(), initialBatchSize);
    }
}
