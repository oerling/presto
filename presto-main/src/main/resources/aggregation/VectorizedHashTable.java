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

import com.facebook.presto.operator.aggregation.VectorizedAggregation.Aggregator;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.ArrayAllocator;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockFlattener;
import com.facebook.presto.spi.block.DictionaryBlock;
    import com.facebook.presto.spi.block.IntArrayBlock;
    import com.facebook.presto.spi.block.LongArrayBlock;
import com.facebook.presto.spi.block.VariableWidthBlock;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import com.google.common.primitives.Ints;
import io.airlift.slice.ByteArrays;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.facebook.presto.spi.type.AbstractLongType;

import static com.facebook.presto.array.Arrays.ensureCapacity;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;

import static com.google.common.base.Verify.verify;

public class VectorizedHashTable
{
    public static final int HASH_SLAB_BYTES = 64 * 1024;
    public static final int ROW_SLAB_BYTES = 64 * 1024;
    public static final int PROBE_WINDOW = 256;
    public static final int LONG_TYPE = 1;
    public static final int INT_TYPE = 2;
    public static final int DOUBLE_TYPE = 3;
    public static final int FLOAT_TYPE = 4;
    public static final int STRING_TYPE = 4;
    public static final int COMPLEX_TYPE = 6;

    public static final int SUM_AGGREGATION = 1;
    
    static Unsafe unsafe;

    static {
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            unsafe = (Unsafe) field.get(null);
            if (unsafe == null) {
                throw new RuntimeException("Unsafe access not available");
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    TableShard[] shards;

    int offset;
    int nullOffset;
    int fixedRowSize;
    List<Type> keys;
    int[] groupByChannels;
    Optional<Integer> hashChannel;
    List<Type> dependent;
    List<Column> columns = new ArrayList();
    List<Column> outputColumns = new ArrayList();
    List<Aggregation> aggregations = new ArrayList();
    int firstLongKey = -1;

    // Input positions to consider. Initially 0, ... last position.
    int[] candidates;
    int numCandidates;
    long[] hashes;
    DECL_ROWS(hitCandidate);
    // The first word of each candidate hit.
    long[] firstWord;
    int[] hitCandidatePositions;
    int numHitCandidates;
    int hitCandidatesSize;
    int[] hitRows;
    int[] actualHits;
    int numHits;
    long[] maskWords;
    // The input position for updating the pairwise corresponding group in groups.
    int[] groupPositions;
    int numGroups;
    // Input positions for which a group must be inserted.
    int[] misses;
    int numMisses;
    int[] toRecheck;
    int numToRecheck;
    long[] insertBloom = new long[64];
    int insertBloomSizeMask = 63;
    boolean unroll;
    OutputState outputState;

    public VectorizedHashTable(
                        List<Type> keys,
                        List<Type> dependent,
                        int[] groupByChannels,
                        Optional<Integer> hashChannel,
                        int[] dependentChannels,
                        List<Aggregator> aggregators,
                        boolean nullableKeys,
                        long initialSize)
    {
        this.groupByChannels = groupByChannels;
        this.hashChannel = hashChannel;
        for (int i = 0; i < keys.size(); i++) {
            Type type = keys.get(i);
            addColumn(type, nullableKeys);
            if (firstLongKey == -1 && type == BIGINT)  {
                firstLongKey = i;
            }
        }
            int nullStart = offset;
        for (int i = 0; i < dependent.size(); i++) {
            addColumn(dependent.get(i), true);
        }
        for (int i = 0; i < aggregators.size(); i++) {
            addAccumulator(aggregators.get(i));
        }
        offsetNulls(nullStart * 8);
        int nullBytes = roundUp(nullOffset, 8) / 8;
        for (int i = keys.size(); i < columns.size(); i++) {
            columns.get(i).offset(nullBytes);
        }
	Column lastColumn = columns.get(columns.size() - 1);
	fixedRowSize = lastColumn.offset + lastColumn.getSize();
	outputColumns.addAll(columns);
    }

    void addColumn(Type type, boolean isNullable)
    {
        Column column = new Column(type, offset, isNullable ? nullOffset : -1);
        columns.add(column);
        offset += column.getSize();
        if (isNullable) {
            nullOffset++;
        }
    }

    private void addAccumulator(Aggregator aggregator)
    {
        String name = aggregator.getName();
        if (name.equals("sum")) {
            int[] channels = aggregator.getInputChannels();
            Type type = aggregator.getType();
            Column column = new Column(type, offset, nullOffset);
            nullOffset++;
            offset += column.getSize();
            aggregations.add(new Aggregation(aggregator, ImmutableList.of(column), SUM_AGGREGATION));
        }
        else {
            throw new UnsupportedOperationException("Aggregate " + name + " is not supported on vectorized path");
        }
    }

    private void offsetNulls(int numBits)
    {
        for (Column column : columns) {
            column.nullOffset += numBits;
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
            this.offset = offset;
            this.nullOffset = nullOffset;
        }
        
        int getSize()
        {
            switch (typeCode) {
            case INT_TYPE:
            case FLOAT_TYPE: return 4;
            default: return 8;
            }
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

    static class Aggregation
    {
        Aggregator aggregator;
        List<Column> columns;
        int opCode;

        Aggregation(Aggregator aggregator, List<Column> columns, int aggregationCode)
        {
            this.aggregator = aggregator;
            this.columns = columns;
            this.opCode = columns.get(0).typeCode + aggregationCode * 100;
        }

        // rows[rowIndices[i] is updated by blocks[xx][inputPositions[i]].
        void addInput(TAKE_ROWS(rows), int[] rowIndices, int[] inputPositions, int numInput, Block[] blocks)
        {
            int[] inputChannels = aggregator.getInputChannels();
            int offset = columns.get(0).offset;
            switch (opCode)
                {
                case (LONG_TYPE + 100 * SUM_AGGREGATION): {
                    Block block = blocks[inputChannels[0]];
                    if (block instanceof LongArrayBlock) {
                        LongArrayBlock typedBlock = (LongArrayBlock) block;
                        int base = typedBlock.getOffsetBase();
                        DECL_PTR(row);
                        for (int i = 0; i < numInput; i++) {
                            int rowIndex = rowIndices[i];
                            int position = inputPositions[i];
                            LOAD_PTR(row, rows, rowIndex);
                            long value = typedBlock.getLongUnchecked(base + position);
                            long valueOnRow = GETLONG(row, offset);
                            SETLONG(row, offset, valueOnRow + value);
                        }
                    break;
                }
                }
                default:
                    throw new UnsupportedOperationException("Aggregate " + opCode + "not supported");

                }
        }
    }

    static void free(long f)
    {
        if (f != 0) {
            /*Unsafe.freeMemory(f)*/;
        }
    }

    static void free(byte[] bytes)
    {
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
        long[] hashes = new long[PROBE_WINDOW];
        long[] rehashEntries = new long[PROBE_WINDOW];
        TableShard[] shards;

        TableShard(int size)
        {
            shards = new TableShard[1];
            shards[0] = this;
	    if (size > 0) {
		initializeHash(size);
	    }
        }

        void initializeHash(int size)
        {
            size = Math.max(size, HASH_SLAB_BYTES / 8);
            int roundedSize = nextPowerOf2(size);
            statusMask = roundedSize - 1;
            capacity = roundedSize / 3 * 2;
            status = new byte[roundedSize / (HASH_SLAB_BYTES / 8)][];
            for (int i = 0; i < status.length; i++) {
                status[i] = getTableSlab();
                Arrays.fill(status[i], (byte) 128);
            }
            entries = new byte[status.length * 8][];
            for (int i = 0; i < entries.length; i++) {
                entries[i] = getTableSlab();
                Arrays.fill(entries[i], (byte) -1);
            }
        }

        byte[] getTableSlab()
        {
            return new byte[HASH_SLAB_BYTES];
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
            for (int i = 0; i < numSlabs; i++) {
                free(slabs[i]);
            }
            slabs = null;
            slabSizes = null;
            slabFilledTo = null;
            numSlabs = 0;
        }


        void rehash(List<Column> columns, int numKeys)
        {
            freeHash();
            initializeHash((statusMask + 1) * 16);
            Column lastColumn = columns.get(columns.size() - 1);
            int fixedRowSize = lastColumn.offset + lastColumn.getSize();
	    int numRows = 0;
            for (int slabIndex = 0; slabIndex < numSlabs; slabIndex++) {
                int slabEnd = slabFilledTo[slabIndex];
                for (int rowIndex = 0; rowIndex < slabEnd; rowIndex += fixedRowSize) {
                    rehashEntries[numRows++] = ENCODE_PTR(slabIndex, rowIndex);
                    if (numRows == PROBE_WINDOW) {
                        hashRows(columns, numKeys, numRows);
                        addRows(numRows);
                        numRows = 0;
                    }
                }
            }
            hashRows(columns, numKeys, numRows);
            addRows(numRows);
        }

        void hashRows(List<Column> columns, int numKeys, int numRows)
        {
            Arrays.fill(hashes, 0, numRows, 0);
            
            for (int columnIndex = 0; columnIndex < numKeys; columnIndex++) {
                Column key = columns.get(columnIndex);
                int offset = key.offset;
                DECL_PTR(row);
                byte[][] slabs0 = slabs;
                for (int i = 0; i < numRows; i++) {
                    DECODE_ENTRY(row, 0, rehashEntries[i]);
                    hashes[i] = AbstractLongType.hash(GETLONG(row, offset));
                }
            }
        }

        void addRows(int numRows)
        {
            for (int i = 0; i < numRows; i++) {
                addHash(hashes[i], rehashEntries[i]);
            }
        }

        void close()
        {
            freeSlabs();
            freeHash();
        }

        void newRow(int size)
        {
            if (numSlabs == 0) {
                slabs = new byte[10][];
                slabFilledTo = new int[10];
                slabSizes = new int[10];
                slabs[0] = getHashSlab();
                numSlabs = 1;
            }
            lastSlabIndex = numSlabs - 1;
            if (slabSizes[lastSlabIndex] - slabFilledTo[lastSlabIndex] < size) {
                newSlab(HASH_SLAB_BYTES);
                lastSlabIndex = numSlabs - 1;
            }
            lastAllocSlabIndex = lastSlabIndex;
            lastAllocOffset = slabFilledTo[lastSlabIndex];
            slabFilledTo[lastSlabIndex] += size;
        }
        
        void newSlab(int size)
        {
            if (slabs.length == numSlabs) {
                growSlabs();
            }
            slabs[numSlabs] = new byte[size];
            slabSizes[numSlabs] = size;
            slabFilledTo[numSlabs] = 0;
            numSlabs++;
        }
        
        void growSlabs()
        {
            slabs = Arrays.copyOf(slabs, slabs.length * 2);
            slabSizes = Arrays.copyOf(slabSizes, slabSizes.length * 2);
            slabFilledTo = Arrays.copyOf(slabFilledTo, slabFilledTo.length * 2);
        }

        byte[] getHashSlab()
        {
            return new byte[HASH_SLAB_BYTES];
        }
        
        void addHash(long hash, long entry)
        {
            int hash0 = (int) hash & statusMask;
            long field = (hash >> 56) & 0x7f;
            byte statusByte = (byte) field;
            USE_TABLE(0, 0);
            for (;;) {
                long hits0;
                LOAD_STATUS(hits0, 0, hash0);
                long free = hits0 & 0x8080808080808080L;
                if (free != 0) {
                    int pos = Long.numberOfTrailingZeros(free) >> 3;
long                     newStatus= hits0 ^ (long) (statusByte | 0x80) << (pos * 8);

STORE_STATUS(0, hash0, newStatus);
STORE_ENTRY(0, hash0 + pos, entry);
                    break;
                }
                hash0 = (hash0 + 1) & statusMask;
            }
            numEntries++;
        }

        long getSize()
        {
            return numSlabs * ROW_SLAB_BYTES + entries.length * HASH_SLAB_BYTES;
        }
    }    

    // blocks are the flattened input Blocks. The dependent and the aggregates are updated and new groups are added as needed. 
    public void groupByInput(Block[] blocks)
    {
        InitializeCandidates(blocks);
        int totalInput = numCandidates;
        for (int window = 0; window < totalInput; window += PROBE_WINDOW) {
            int windowEnd = Math.min(totalInput, window + PROBE_WINDOW);
            for (int i = window; i < windowEnd; i++) {
                candidates[i - window] = i;
            }
            numCandidates = windowEnd - window;
            int currentProbe = 0;
            int firstKeyOffset;
            if (firstLongKey != -1) {
                firstKeyOffset = columns.get(firstLongKey).offset;
            }
            else {
                firstKeyOffset = columns.get(0).offset;
            }
                while (numCandidates > 0) {
                USE_TABLE(0, 0);
                long tempHash;
                long encodedPayload;
                DECL_PROBE(0);
                if (unroll) {
                    for (; currentProbe + 4 < numCandidates; currentProbe += 4) {
                        
                    }
                }
                for (; currentProbe < numCandidates; currentProbe++) {
                    PRE_PROBE(0, 0);
                    FIRST_PROBE(0, 0);
                    FULL_PROBE(0, 0);
                }
                compareKeys(blocks);
                insertMisses(blocks);
                updateGroups(blocks);
                System.arraycopy(toRecheck, 0, candidates, 0, numToRecheck);
                numCandidates = numToRecheck;
            }
        }
    }

    private class OutputState
    {
	int outputBatch = 1024;
	DECL_ROWS(rows);
	int numOutput;
	int currentShard;
	int currentSlab;
	int currentOffset;
	
	OutputState()
	{
	    ALLOC_ROWS(rows, outputBatch);
	}

	void reset()
	{
	    currentShard = 0;
	    currentSlab = 0;
	    currentOffset = 0;
	    numOutput = 0;
	}
	
	Block getBlock(int from, int to, Column column)
	{
	    int offset = column.offset;
	    switch (column.typeCode) {
	    case LONG_TYPE:
	    case DOUBLE_TYPE: {
		long[] array = new long[to - from];
		DECL_PTR(ptr);
		for (int i = from; i < to; i++) {
		    LOAD_PTR(ptr, rows, i);
		    array[i - from] = GETLONG(ptr, offset);
		}
		return new LongArrayBlock(to - from, Optional.empty(), array);
	    }
	    default: {
		throw new UnsupportedOperationException("Column type not supported");
	    }
	    }
	}

	void nextBatch()
	{
	    TableShard shard = shards[currentShard];
	    DECL_PTR(ptr);
	    while (true) {
		ptrBytes = shard.slabs[currentSlab];
		ptrOffset = currentOffset;
		rowsBytes[numOutput] = ptrBytes;
		rowsOffset[numOutput] = ptrOffset;
		numOutput++;
		currentOffset += fixedRowSize;
		if (currentOffset >= shard.slabFilledTo[currentSlab]){
		    currentSlab++;
		    currentOffset = 0;
		    if (currentSlab == shard.numSlabs) {
			currentShard++;
			currentSlab = 0;
			if (currentShard == shards.length) {
			    return;
			}
		    }
		}
		if (numOutput == outputBatch) {
		    return;
		}
	    }
	    }

	Page getOutput()
	{
	    if (outputState == null) {
		outputState = new OutputState();
	    }
	    if (outputDone()) {
		return null;
	    }
	    nextBatch();
	    if (outputDone() && numOutput == 0) {
		return null;
	    }
	    Block[] blocks= new Block[outputColumns.size()];
	    for (int i = 0; i < outputColumns.size(); i++) {
		Column column = outputColumns.get(i);
		blocks[i] = getBlock(0, numOutput, column);
	    }
	    return new Page(numOutput, blocks);
	}	

	boolean outputDone()
	{
	    return currentShard >= shards.length;
	}
    }    

    public Page getOutput()
    {
	if (outputState == null) {
	    outputState = new OutputState();
	}
	return outputState.getOutput();
    }

    private void compareKeys(Block[] blocks)
    {
        Arrays.fill(maskWords, 0, roundUp(numHitCandidates, 64) / 64, -1);
        if (firstLongKey != -1) {
            Block probeBlock = blocks[groupByChannels[firstLongKey]];
            if (probeBlock instanceof LongArrayBlock) {
                LongArrayBlock longBlock = (LongArrayBlock) probeBlock;
                int base = longBlock. getOffsetBase();
                for (int windowBase = 0; windowBase < numHitCandidates; windowBase += 64) {
                    int windowEnd = Math.min(windowBase + 64, numHitCandidates);
                    long maskWord = maskWords[windowBase / 64];
                    long mask = 1;
                    for (int i = windowBase; i < windowEnd; i++) {
                    if (firstWord[i] != longBlock.getLongUnchecked(i + base)) {
                    maskWord &= ~mask;
                }
                mask = mask << 1;
                    }
                    maskWords[windowBase / 64] = maskWord;
                }
            }
            else {
                throw new UnsupportedOperationException("Only LongArrayBlocks for fast aggregation");
            }
        }
        // Divide maskWords into hits and misses. Expect mostly hits.
        hitRows = ensureCapacity(hitRows, numCandidates);
        for (int windowBase = 0; windowBase < numHitCandidates; windowBase += 64) {
            int windowEnd = Math.min(windowBase + 64, numHitCandidates);
            long maskWord = maskWords[windowBase / 64];
            long mask = 1;
            for (int i = windowBase; i < windowEnd; i++) {
                if ((maskWord & mask) != 0) {
                    hitRows[numHits] = i;
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

    private void insertMisses(Block[] blocks)
    {
        TableShard shard = shards[0];
        if (shard.numEntries > shard.capacity) {
            shard.rehash(columns, keys.size());
        }
        Arrays.fill(insertBloom, 0);
        toRecheck = ensureCapacity(toRecheck, numMisses);
        numToRecheck = 0;
        int initialCandidates = numHitCandidates;
        for (int i = 0; i < numMisses; i++) {
            int row = misses[i];
            long hash = hashes[row];
            long bloomMask = BF_MASK(hash);
            int bloomIndex = (int) hash & insertBloomSizeMask;
            if ((insertBloom[bloomIndex] & bloomMask) == bloomMask) {
                toRecheck[numToRecheck++] = row;
                continue;
            }
            insertBloom[bloomIndex] |= bloomMask;
            DECL_PTR(newRow);
            long encodedNewRow;
            shard.newRow(fixedRowSize);
            GET_NEW_ROW(newRow, encodedNewRow, shard);
            hitRows[numHits] = numHitCandidates;
            actualHits[numHits++] = row;
            ADD_ROW(hitCandidate, row, numHitCandidates, newRow);
            shard.addHash(hash, encodedNewRow);
        }
        initializeNewRows(initialCandidates, numHitCandidates, blocks);
    }

    private void initializeNewRows(int from, int to, Block[] blocks)
    {
	    USE_TABLE(0, 0);
	    for (int columnIndex = 0; columnIndex < keys.size(); columnIndex++) {
            int offset = columns.get(columnIndex).offset;
            Block block = blocks[groupByChannels[columnIndex]];
	    int base = block.getOffsetBase();
            DECL_PTR(ptr);
            for (int i = from; i < to; i++) {
                int row = hitCandidatePositions[i];
                LOAD_PTR(ptr, hitCandidate, i);
                SETLONG(ptr, offset, block.getLongUnchecked(row + base));
            }
        }
    }

    void InitializeCandidates(Block[] blocks)
    {
	if (shards == null) {
	    shards = new TableShard[1];
	    shards[0] = new TableShard(8192);
	}
        numCandidates = blocks[0].getPositionCount();
        candidates = ensureCapacity(candidates, numCandidates);
	misses = ensureCapacity(misses, numCandidates);
        hashes = ensureCapacity(hashes, numCandidates);
        if (hashChannel.isPresent()) {
            Block hashBlock = blocks[hashChannel.get()];
            int base = hashBlock.getOffsetBase();
            for (int i = 0; i < numCandidates; i++) {
                hashes[i] = hashBlock.getLongUnchecked(i + base);
            }
        }
        if (hitCandidatesSize == 0) {
            actualHits = new int[PROBE_WINDOW];
            hitCandidatesSize = PROBE_WINDOW * 3;
            ALLOC_ROWS(hitCandidate, hitCandidatesSize);
            hitCandidatePositions = new int[hitCandidatesSize];
            firstWord = new long[hitCandidatesSize];
            maskWords = new long[(hitCandidatesSize + 64) / 64];
        }
    }

    private void growHitCandidates()
    {
        hitCandidatesSize *= 2;
        GROW_ROWS(hitCandidate, hitCandidatesSize);
        hitCandidatePositions = Arrays.copyOf(hitCandidatePositions, hitCandidatesSize);
        firstWord = Arrays.copyOf(firstWord, hitCandidatesSize);
        maskWords = new long[(hitCandidatesSize + 64) / 64];
    }
    
    void compareColumn(Column column, Block block, TAKE_ROWS(hitRows), int numHits, int[] candidateInputs, long[] maskWords)
    {
        int blockBase = block.getOffsetBase();
        int columnOffset = column.offset;
	DECL_PTR(row);
        for (int base = 0; base  < numHits; base += 64) {
	    int end = Math.min(numHits, base + 64);
            long maskWord = maskWords[base / 64];
            long mask = 1;
            for (int i = base; i < end; i++) {
                if ((maskWord & mask) != 0) {
                    LOAD_PTR(row, hitCandidate, i);
                    if (GETLONG(row, columnOffset) != block.getLongUnchecked(candidateInputs[i] + blockBase)) {
                        maskWord &= ~mask;
                    }
                    mask = mask << 1;
                }
                maskWords[base / 64] = maskWord;
            }
                }
    }

    void updateGroups(Block[] blocks)
    {
        for (Aggregation aggregation : aggregations) {
            aggregation.addInput(PASS_ROWS(hitCandidate), hitRows, actualHits, numHits, blocks);
        }
    }
    
    // The positions in keys given by candidates are probed. The
    // positions that hit at least one row are returned in candidates
    // and the corresponding first match row is in hits. The number of
    // hits is returned.
    public int hashProbeInput(Block[] keys, int[] candidates, TAKE_ROWS(hits))
    {
	return 0;
    }


    public long getEstimatedSize()
    {
        long size = 0;
        for (TableShard shard : shards) {
            if (shard != null) {
                size += shard.getSize();
            }
        }
	return size;
    }
    
    public long getGroupCount()
    {
        long size = 0;
        for (TableShard shard : shards) {
            if (shard != null) {
                size += shard.numEntries;
            }
        }
	return size;
    }

    public int getCapacity()
    {
        int size = 0;
        for (TableShard shard : shards) {
            if (shard != null) {
                size += shard.capacity;
            }
        }
	return size;
    }


    
    public List<Type> getTypes()
    {
        return keys;
    }

    public void close()
    {
        for (TableShard shard : shards) {
            shard.close();
        }
        shards = null;
    }
    
    static int getCodeForType(Type type)
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

    private static int nextPowerOf2(int number)
    {
        return Integer.bitCount(number) == 1 ? number : Integer.highestOneBit(number << 1);
    }
}
