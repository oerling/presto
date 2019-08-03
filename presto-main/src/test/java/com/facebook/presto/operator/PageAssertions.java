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

import com.facebook.presto.block.BlockAssertions.Encoding;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.block.BlockAssertions.Encoding.DICTIONARY;
import static com.facebook.presto.block.BlockAssertions.Encoding.RUN_LENGTH;
import static com.facebook.presto.block.BlockAssertions.assertBlockEquals;
import static com.facebook.presto.block.BlockAssertions.createBlockWithType;
import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createNullBlock;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static org.testng.Assert.assertEquals;

public final class PageAssertions
{
    private PageAssertions()
    {
    }

    public static void assertPageEquals(List<? extends Type> types, Page actualPage, Page expectedPage)
    {
        assertEquals(types.size(), actualPage.getChannelCount());
        assertEquals(actualPage.getChannelCount(), expectedPage.getChannelCount());
        assertEquals(actualPage.getPositionCount(), expectedPage.getPositionCount());
        for (int i = 0; i < actualPage.getChannelCount(); i++) {
            assertBlockEquals(types.get(i), actualPage.getBlock(i), expectedPage.getBlock(i));
        }
    }

    static Page createPage(List<Type> types, int positionCount, boolean allowNulls)
    {
        return createPage(types, positionCount, true, false, allowNulls, false, ImmutableList.of());
    }

    static Page createDictionaryPage(List<Type> types, int positionCount, boolean allowNulls)
    {
        return createPage(types, positionCount, true, false, allowNulls, false, ImmutableList.of(DICTIONARY));
    }

    static Page createRLEPage(List<Type> types, int positionCount, boolean allowNulls)
    {
        return createPage(types, positionCount, true, false, allowNulls, false, ImmutableList.of(RUN_LENGTH));
    }

    static Page createPage(List<Type> types, int positionCount, boolean addPreComputedHashBlockAtBegin, boolean addNullBlockAtEnd, boolean useBlockView, List<Encoding> wrappings)
    {
        return createPage(types, positionCount, addPreComputedHashBlockAtBegin, addNullBlockAtEnd, true, useBlockView, wrappings);
    }

    private static Page createPage(
            List<Type> types,
            int positionCount,
            boolean addPreComputedHashBlock,
            boolean addNullBlock,
            boolean allowNulls,
            boolean useBlockView,
            List<Encoding> wrappings)
    {
        int channelCount = types.size();
        int preComputedChannelCount = (addPreComputedHashBlock ? 1 : 0);
        int nullChannelCount = (addNullBlock ? 1 : 0);

        Block[] blocks = new Block[channelCount + preComputedChannelCount + nullChannelCount];

        if (addPreComputedHashBlock) {
            blocks[0] = createLongsBlock(positionCount, false);
        }

        for (int i = 0; i < channelCount; i++) {
            blocks[i + preComputedChannelCount] = createBlockWithType(types.get(i), positionCount, allowNulls, useBlockView, wrappings);
        }

        if (addNullBlock) {
            blocks[channelCount + preComputedChannelCount] = createNullBlock(BIGINT, positionCount);
        }

        return new Page(positionCount, blocks);
    }

    static Page mergePages(List<Type> types, List<Page> pages)
    {
        PageBuilder pageBuilder = new PageBuilder(types);
        int totalPositionCount = 0;
        for (Page page : pages) {
            for (int i = 0; i < types.size(); i++) {
                BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(i);
                Block block = page.getBlock(i);
                for (int position = 0; position < page.getPositionCount(); position++) {
                    if (block.isNull(position)) {
                        blockBuilder.appendNull();
                    }
                    else {
                        block.writePositionTo(position, blockBuilder);
                    }
                }
            }
            totalPositionCount += page.getPositionCount();
        }
        pageBuilder.declarePositions(totalPositionCount);
        return pageBuilder.build();
    }

    static List<Type> buildTypes(List<Type> types, boolean addPreComputedHashBlock, boolean addNullBlock)
    {
        ImmutableList.Builder<Type> newTypes = ImmutableList.builder();

        if (addPreComputedHashBlock) {
            newTypes.add(BIGINT);
        }

        newTypes.addAll(types);

        if (addNullBlock) {
            newTypes.add(BIGINT);
        }

        return newTypes.build();
    }
}
