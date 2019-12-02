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
package com.facebook.presto.operator.aggregation;

#include "hash.h"

    import com.facebook.presto.spi.block.Block;
    import com.facebook.presto.spi.block.DictionaryBlock;
    import com.facebook.presto.spi.block.IntArrayBlock;
    import com.facebook.presto.spi.block.LongArrayBlock;
import com.facebook.presto.spi.block.VariableWidthBlock;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

class VectorizedHashTable
{
    public static final int HASH_SLAB_BYTES = 64 * 1024;
    public static final int ROW_SLAG_BYTES = 64 * 1024;
    public static final int PROBE_WINDOW = 256;
    public static final int LONG_TYPE = 1;
    public static final int INT_TYPE = 2;
    public static final int DOUBLE_TYPE = 3;
    public static final int FLOAT_TYPE = 4;
    public static final int STRING_TYPE = 4;
    public static final int COMPLEX_TYPE = 6;
        
    TableShard[] shards;

    int offset;
    int nullOffset;
    int fixedRowLength;
    List<Type> keys;
    List<Type> dependent;
    List<AccumulatorFactory> accumulators;
    List<Column> columns = new ArrayList();

    // Input positions to consider. Initially 0, ... last position.
    int[] candidates;
    DECL_ROWS(hitCandidate);
    // The first word of each candidate hit.
    long[] firstWord;
    int[] hitCandidatePositions;
    int numHitCandidates;
    int hitCandidateSize;
    int[] actualHits;
    int numHits;
    long[] maskWords;
    // The input position for updating the pairwise corresponding group in groups.
    int[] groupPositions;
    int numGroups;
    // Input positions for which a group must be inserted.
    int[] misses;
    int numMisses;
    int toRechek;
    int numToRecheck;
    long[] insertBloom = new long[64];
    int insertBloomSizeMask = 63;

    VectorizedHashTable(
                        List<Type> keys,
                        List<Type> dependent,
                        int[] groupByChannels,
                        Optional<Integer> hashChannel,
                        int[] dependentChannels,
                        List<AccumulatorFactory> accumulators,
                        boolean nullableKeys,
                        long initialSize)
    {
        this.groupByChannels = groupByChannels;
        for (int i = 0; i < keys.size(); i++) {
            addColumn(keys.get(i), nullableKeys);
        }
        int nullStart = offset;
        for (int i = 0; i < dependent.size(); i++) {
            addColumn(dependent.get(i), true);
        }
        for (int i = 0; i < accumulators.size(); i++) {
            addAccumulator(accumulators.get(i));
        }
        offsetNulls(nullStart * 8);
        int nullBytes = roundUp(nullOffset, 8) / 8;
        for (int i = keys.size(); i < columns.size(); i++) {
            columns.get(i).offset(nullBytes);
        }
    }

    public static class Column
    {
        final int typeCode;
        final Type type;
        int offset;
        int nullOffset;
        boolean hasNull;

        Column(Type type, int offset, int nullOffset)
        {
            this.type = type;
            this.typeCode = getCodeForType(type);
        }
        
        void offsetNull(int bits)
        {
            if (nullOffset != -1) {
                nullOffset += bits;
            }
        }
        
        void offset(int bytes)
        {
            offset += bytes;
        }
    }

    static class TableShard
    {
        DECL_STATUS(status);
        DECL_ENTRIES(entries);
        DECL_SLABS(slabs);
        int statusMask;
        int numEntries;
        int capacity;
        int numSlabs;
        int[] slabSizes; 
        int[]slabFilledTo;
        int lastSlabIndex;
        int lastAllocOffset;
        int lastAllocSlabIndex;
        TableShard[] shards;

        TableShard()
        {
            shards = new TableShard[1];
            shards[0] = this;
        }

        void initializeHash(int size)
        {
            int roundedSize = roundUp(size, HASH_SLAB_BYTES);
            status = new byte[roundedSize / HASH_SLAB_BYTES][];
            for (int i = 0; i < status.length; i++) {
                status[i] = getTableSlab();
            }
        }

        byte[] getTableSlab()
        {
            return new byte[HASH_SLAB_SIZE];
        }

        void freeHash()
        {
            for (int i = 0; i < status.length; i++) {
                free(status[i]);
            }
            status = null;
            for (int i = 0; i < entries.length; i++) {
                free(entries[i]);
            }
            entries = null;

        }

        void freeSlabs()
        {
            
        }

        void newRow(int size)
        {
            if (numSlabs == 0) {
                slabs = new byte[10][];
                slabFill = new int[10];
                slabSizes = new int[10];
                slabs[0] = getHashSlab();
                numSlabs = 1;
            }
            lastSlab = numSlabs - 1;
            if (slabSizes[lastSlab] - slabFill[lastSlab] < size) {
                newSlab(HASH_SLAB_SIZE);
                lastSlab = numSlabs - 1;
            }
            lastAllocSlabIndex = lastSlab;
            lastAllocOffset = slabFill[lastSlab];
            slabFill[lastSlab} += size;
        }
        
        void newSlab(int size)
        {
            if (slabs.length == numSlabs) {
                growSlabs();
            }
            slabs.numSlabs = new byte[size];
            slabSizes[numSlabs] = size;
            slabFill[numSlabs] = 0;
            numSlabs++;
        }
        
        void growSlabs()
        {
            slabs = Arrays.copyOf(slabs, slabs.length * 2);
            slabSizes = Arrays.copyOf(slabSizes, slabSizes.length * 2);
            slabFill = Arrays.copyOf(slabFill, slabFill.length * 2);
        }
        
        void addHash(long hash, long entry)
        {
            int index = (int) hash & statusMask;
            long field = (hash >> 56) & 0x7f;
            byte statusByte = (byte) field;
            USE_TABLE(0, 0);
            for (;;) {
                long hits0;
                LOAD_STATUS(hits, 0, index);
                long free = hits0 & 0x8080808080808080L;
                if (free != 0) {
                    int pos = Long.numberOfTrailingZeros(free) >> 3;
long                     newStatus= hits0 ^ (long) (statusByte | 0x80) << (pos * 8);

STORE_STATUS(0, index, newStatus);
STORE_ENTRY(0, index + pos, entry);
                    break;
                }
                index = (index + 1) & statusMask;
            }
        }

        void free(long f) {
            if (f != 0) {
                Unsafe.freeMemory(f);
            }
        }

        void free(byte[])
        {
        }
    }    

    // blocks are the flattened input Blocks. The dependent and the aggregates are updated and new groups are added as needed. 
    void groupByInput(Block[] blocks)
    {
        InitializeCandidates(blocks);
        int totalInput = numCandidates;
        for (window = 0; window < totalInput; window += PROBE_WINDOW) {
            int windowEnd = Math.min(totalInput, window + PROBE_WINDOW);
            for (int i = window; i < windowEnd; i++) {
                candidates[i - window] = i;
            }
            numCandidates = windowEnd - window;
            int currentProbe = 0;
            int firstKeyOffset = keys[0].offset;
            while (numCandidates > 0) {
                USE_TABLE(0, 0);
                DECL_PROBE(0);
                if (unroll) {
                    for (; currentProbe + 4 < numCandidates; currentProbe += 4) {
                        
                    }
                }
                for (; currentProbe < numCandidates; currentProbe++) {
                    PREGET(0, 0);
                    FIRST_PROBE(0, 0);
                    FULL_PROBE(0, 0);
                }
                compareKeys(blocks);
                insertMisses(blocks);
                updateGroups();
                System.arraycopy(toRecheck, 0, candidates, 0, numToRecheck);
                numCandidates = numToRecheck;
            }
        }
    }
    
    private void compareKeys(Block[] blocks)
    {
        Arrays.fill(maskWords, -1, 0, roundUp(numHitCandidates, 64));
        if (firstLongKey != -1) {
            Block probeBlock = blocks[groupByChannels[firstLongKey]];
            if (probeBlock instanceof LongArrayBlock) {
                LongArrayBlock longBlock = (LongArrayBlock) probeBlock;
                int base = longBlock. getOffsetBase();
                for (int windowBase = 0; windowBase < numHitCandidates; windowBase += 64) {
                    int windowEnd = Math.min(windowStart + 64, numHitCandidates);
                    maskWord = maskWords[windowStart / 64];
                    long mask = 1;
                    for (int i = windowBase; i < windowEnd; i++) {
                    if (firstWord[i] != longBlock.getLongUnchecked(i + base)) {
                    maskWord &= ~mask;
                }
                mask = mask << 1;
                    }
                    maskwords[windowStart / 64] = maskWord;
                }
            }
            else {
                throw UnsupportedOperationException("Only LongArrayBlocks for fast aggregation");
            }
        }
        // Divide maskWords into hits and misses. Expect mostly hits.
        for (int windowBase = 0; windowBase < numHitCandidates; windowBase += 64) {
            int windowEnd = Math.min(windowStart + 64, numHitCandidates);
            maskWord = maskWords[windowStart / 64];
            long mask = 1;
            for (int i = windowBase; i < windowEnd; i++) {
                if (maskWord & mask != 0) {
                    actualHits[numHits++] = hitCandidatePositions[i];
                }
                mask = mask << 1;
            }
        }
        // The misses are the candidates that are not in hits.
        numMisses = 0;
        int hitIndex = 0;
        for (int i = 0; i < numCandidates; i++) {
            int candidate = candidates[i];
            if (hitIndex >= numHits) {
                misses[numMisses++] = candidate;
                continue;
            }
            if (candidate < actualHits[hitIndex]) {
                misses[numMisses++] = candidate;
                continue;
            }
            if (candidate == actualHits[hitIndex]) {
                hitIndex++;
                continue;
            }
            else {
                verify(false, "candidates out of order");
            }
        }
    }

    private void insertMisses(Block[] bloccks)
    {
        TableShard shard = shards[0];
        if (shard.numEntries > shard.capacity) {
            rehash(shard);
        }
        Arrays.fill(insertBloom, 0);
        toRecheck = ensureCapacity(toRechek, numHitCandidates);
        numToRecheck = 0;
        int initialCandidates = numHitCandidates;
        for (int i = 0; i < numMisses; i++) {
            int row = misses[i];
            long hash = hashes[row];
            long mask = BF_MASK(hash);
            int bloomIndex = (int) hash & bloomSizeMask;
            if (insertBloom[bloomIndex] & bloomMask == bloomMask) {
                toRecheck[numToRecheck++] = row;
                continue;
            }
            insertBloom[bloomIndex] |= bloomMask;
            DECL_PTR(newRow);
            long encodedNewRow;
            shard.addRow(fixedRowSize);
            GET_NEW_ROW(newRow, encodedNewRow, shard);
            actualHits[numHits++] = numHitCandidates;
            ADD_ROW(hitCandidate, row, numHitCandidates, newRow);
            shard.addHash(hash, encodedNewRow);
        }
        initializeNewRows(initialCandidates, numHitCandidates, blocks);
    }

    private void initializeNewRows(int from, int to, Block[] blocks)
    {
        
        for (int columnIndex = 0; columnIndex < keys.size(); columnIndex++) {
            Block block = blocks[groupByChannels[columnIndex]];
            DECL_PTR(ptr);
            for (int i = from; i < to; i++) {
                int row = hitCandidatePositions[i];
                LOAD_ROW(ptr, hitCandidate, i);
                SETLONG(ptr, block.getLong(row + base));
            }
        }
    }

    void InitializeCandidates(Block[] blocks)
    {
        numCandidates = blocks[0].getPositionCount();
        candidates = ensureCapacity(candidates, numCandidates);
        hashes = ensureCapacity(hashes, numCandidates);
        if (hitCandidatesSize == 0) {
            actualHits = new int[PROBE_WINDOW];
            hitCandidatesSize = PROBE_WINDOW * 3;
            ALLOC_ROWS(hitCandidate, hitCandidateSize);
            hitCandidatePositions = new int[hitCandidateSize];
            firstWord = new long[hitCandidateSize];
            maskWords = new long[(hitCandidateSize + 64) / 64]
        }
    }

    private void growHitCandidates()
    {
        hitCandidateSize *= 2;
        GROW_ROWS(hitCandidate, hitCandidateSize);
        hitCandidatePositions = Arrays.copyOf(hitCandidatePositions, hitCandidateSize);
        firstWord = Arrays.copyOf(firstWord, hitCandidateSize);
        amaskWords = new long[(hitCandidateSize + 64) / 64]
    }
    
    void compareColumn(Column column, Block block, TAKE_ROWS(hitRows), int numHits, int[] candidateInputs, long[] maskWords)
    {
        int blockBase = block.getOffsetBase();
        int columnOffset = column.getOffset();

        for (int base = 0; base  < numHits; base += 64) {
            mask = maskWords[i / 64];
            long mask = 1;
            for (int i = base; i < end; i++) {
                if (maskWord & mask != 0) {
                    LOAD_ROW(row, hitCandidates, i);
                    if (GET_LONG(row, columnOffset) != block.getLongUnchecked(candidateInputs[i] + blockBase)) {
                        maskWord &= ~mask;
                    }
                    mask = mask << 1;
                }
                maskWords[i / 64] = maskWord;
            }
                }
    }


    
    // The positions in keys given by candidates are probed. The
    // positions that hit at least one row are returned in candidates
    // and the corresponding first match row is in hits. The number of
    // hits is returned.
    int hashProbeInput(Block[] keys, int[] candidates, TAKE_ROWS(hits))
    {
    }


    public long getEstimatedSize()
    {
        long size = 0;
        for (TableShard shard : shards) {
            if (shard != null) {
                size += shard.getSize();
            }
        }
    }
    
    public List<Type> getTypes()
    {
        return keys;
    }
    
    static getCodeForType(Type type)
    {
        if (type == BIGINT) {
            return LONG_TYPE;
        }
        if (type == INTEGER) {
            return INT_TYPE;
        }
        if (type == DOUBLE) {
            return DOUBLE_TYPE;
        }
        if (type == REAL) {
            return FLOAT_TYPE;
        }
        if (isVarcharType(type)) {
            return STRING_TYPE;
        }
        return COMPLEX_TYPE;
    }

    static int roundUp(int number, int upTo)
    {
        return ((number + upTo - 1) / upTo) * upTo;
    }
}        

