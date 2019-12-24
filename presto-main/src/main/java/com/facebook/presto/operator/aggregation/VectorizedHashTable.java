package com.facebook.presto.operator.aggregation;

import static com.facebook.presto.array.Arrays.ensureCapacity;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static com.google.common.base.Verify.verify;

import com.facebook.presto.operator.UncheckedByteArrays;
import com.facebook.presto.operator.aggregation.VectorizedAggregation.Aggregator;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.LongArrayBlock;
import com.facebook.presto.spi.type.AbstractLongType;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.ByteArrays;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import sun.misc.Unsafe;

public class VectorizedHashTable {
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
    } catch (Exception e) {
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

  int[] candidates;
  int numCandidates;
  long[] hashes;
  byte[][] hitCandidateBytes;
  int[] hitCandidateOffset;;

  long[] firstWord;
  int[] hitCandidatePositions;
  int numHitCandidates;
  int hitCandidatesSize;
  int[] hitRows;
  int[] actualHits;
  int numHits;
  long[] maskWords;

  int[] groupPositions;
  int numGroups;

  int[] misses;
  int numMisses;
  int[] toRecheck;
  int numToRecheck;
  long[] insertBloom = new long[64];
  int insertBloomSizeMask = 63;
  boolean unroll = true;
  OutputState outputState;

  public VectorizedHashTable(
      List<Type> keys,
      List<Type> dependent,
      int[] groupByChannels,
      Optional<Integer> hashChannel,
      int[] dependentChannels,
      List<Aggregator> aggregators,
      boolean nullableKeys,
      long initialSize) {
    this.groupByChannels = groupByChannels;
    this.hashChannel = hashChannel;
    this.keys = keys;
    for (int i = 0; i < keys.size(); i++) {
      Type type = keys.get(i);
      addColumn(type, nullableKeys);
      if (firstLongKey == -1 && type == BIGINT) {
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
    if (columns.size() == keys.size()) {

      fixedRowSize += nullBytes;
    }
    outputColumns.addAll(columns);
  }

  void addColumn(Type type, boolean isNullable) {
    Column column = new Column(type, offset, isNullable ? nullOffset : -1);
    columns.add(column);
    offset += column.getSize();
    if (isNullable) {
      nullOffset++;
    }
  }

  private void addAccumulator(Aggregator aggregator) {
    String name = aggregator.getName();
    if (name.equals("sum")) {
      int[] channels = aggregator.getInputChannels();
      Type type = aggregator.getType();
      Column column = new Column(type, offset, nullOffset);
      nullOffset++;
      offset += column.getSize();
      columns.add(column);
      aggregations.add(new Aggregation(aggregator, ImmutableList.of(column), SUM_AGGREGATION));
    } else {
      throw new UnsupportedOperationException(
          "Aggregate " + name + " is not supported on vectorized path");
    }
  }

  private void offsetNulls(int numBits) {
    for (Column column : columns) {
      column.nullOffset += numBits;
    }
  }

  public static class Column {
    final int typeCode;
    final Type type;
    int offset;
    int nullOffset;
    boolean hasNull;

    Column(Type type, int offset, int nullOffset) {
      this.type = type;
      this.typeCode = getCodeForType(type);
      this.offset = offset;
      this.nullOffset = nullOffset;
    }

    int getSize() {
      switch (typeCode) {
        case INT_TYPE:
        case FLOAT_TYPE:
          return 4;
        default:
          return 8;
      }
    }

    void offsetNull(int bits) {
      if (nullOffset != -1) {
        nullOffset += bits;
      }
    }

    void offset(int bytes) {
      offset += bytes;
    }
  }

  static class Aggregation {
    Aggregator aggregator;
    List<Column> columns;
    int opCode;

    Aggregation(Aggregator aggregator, List<Column> columns, int aggregationCode) {
      this.aggregator = aggregator;
      this.columns = columns;
      this.opCode = columns.get(0).typeCode + aggregationCode * 100;
    }

    void addInput(
        byte[][] rowsBytes,
        int[] rowsOffset,
        int[] rowIndices,
        int[] inputPositions,
        int numInput,
        Block[] blocks) {
      int[] inputChannels = aggregator.getInputChannels();
      int offset = columns.get(0).offset;
      switch (opCode) {
        case (LONG_TYPE + 100 * SUM_AGGREGATION):
          {
            Block block = blocks[inputChannels[0]];
            if (block instanceof LongArrayBlock) {
              LongArrayBlock typedBlock = (LongArrayBlock) block;
              int base = typedBlock.getOffsetBase();
              byte[] rowBytes = null;
              int rowOffset = -1;
              ;
              for (int i = 0; i < numInput; i++) {
                int rowIndex = rowIndices[i];
                int position = inputPositions[i];
                {
                  rowBytes = rowsBytes[rowIndex];
                  rowOffset = rowsOffset[rowIndex];
                }
                ;
                long value = typedBlock.getLongUnchecked(base + position);
                long valueOnRow =
                    UncheckedByteArrays.getLongUnchecked(rowBytes, rowOffset + offset);
                ByteArrays.setLong(rowBytes, rowOffset + offset, valueOnRow + value);
              }
              break;
            }
          }
        default:
          throw new UnsupportedOperationException("Aggregate " + opCode + "not supported");
      }
    }
  }

  static void free(long f) {
    if (f != 0) {;
    }
  }

  static void free(byte[] bytes) {}

  static class TableShard {
    byte[][] status;
    byte[][] entries;
    byte[][] slabs;
    int statusMask;
    int numEntries;
    int capacity;
    int numSlabs;
    int[] slabSizes;
    int[] slabFilledTo;
    int lastSlabIndex;
    int lastAllocOffset;
    int lastAllocSlabIndex;
    long[] hashes = new long[PROBE_WINDOW];
    long[] rehashEntries = new long[PROBE_WINDOW];
    TableShard[] shards;

    TableShard(int size) {
      shards = new TableShard[1];
      shards[0] = this;
      if (size > 0) {
        initializeHash(size);
      }
    }

    void initializeHash(int size) {
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

    byte[] getTableSlab() {
      return new byte[HASH_SLAB_BYTES];
    }

    void freeHash() {
      for (int i = 0; i < status.length; i++) {
        free(status[i]);
      }
      status = null;
      for (int i = 0; i < entries.length; i++) {
        free(entries[i]);
      }
      entries = null;
    }

    void freeSlabs() {
      for (int i = 0; i < numSlabs; i++) {
        free(slabs[i]);
      }
      slabs = null;
      slabSizes = null;
      slabFilledTo = null;
      numSlabs = 0;
    }

    void rehash(List<Column> columns, int numKeys) {
      freeHash();
      initializeHash((statusMask + 1) * 16);
      Column lastColumn = columns.get(columns.size() - 1);
      int fixedRowSize = lastColumn.offset + lastColumn.getSize();
      int numRows = 0;
      for (int slabIndex = 0; slabIndex < numSlabs; slabIndex++) {
        int slabEnd = slabFilledTo[slabIndex];
        for (int rowIndex = 0; rowIndex < slabEnd; rowIndex += fixedRowSize) {
          rehashEntries[numRows++] = (((long) (slabIndex) << 24) + (rowIndex));
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

    void hashRows(List<Column> columns, int numKeys, int numRows) {
      Arrays.fill(hashes, 0, numRows, 0);

      for (int columnIndex = 0; columnIndex < numKeys; columnIndex++) {
        Column key = columns.get(columnIndex);
        int offset = key.offset;
        byte[] rowBytes = null;
        int rowOffset = -1;
        ;
        byte[][] slabs0 = slabs;
        for (int i = 0; i < numRows; i++) {
          {
            rowBytes = slabs0[(int) (rehashEntries[i] >> 24)];
            rowOffset = (int) rehashEntries[i] & 0xffffff;
          }
          ;
          long hash =
              AbstractLongType.hash(
                  UncheckedByteArrays.getLongUnchecked(rowBytes, rowOffset + offset));
          hashes[i] = columnIndex == 0 ? hash : hashMix(hashes[i], hash);
        }
      }
    }

    void addRows(int numRows) {
      for (int i = 0; i < numRows; i++) {
        addHash(hashes[i], rehashEntries[i]);
      }
    }

    void close() {
      freeSlabs();
      freeHash();
    }

    void newRow(int size) {
      if (numSlabs == 0) {
        slabs = new byte[10][];
        slabFilledTo = new int[10];
        slabSizes = new int[10];
        newSlab(HASH_SLAB_BYTES);
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

    void newSlab(int size) {
      if (slabs.length == numSlabs) {
        growSlabs();
      }
      slabs[numSlabs] = new byte[size];
      slabSizes[numSlabs] = size;
      slabFilledTo[numSlabs] = 0;
      numSlabs++;
    }

    void growSlabs() {
      slabs = Arrays.copyOf(slabs, slabs.length * 2);
      slabSizes = Arrays.copyOf(slabSizes, slabSizes.length * 2);
      slabFilledTo = Arrays.copyOf(slabFilledTo, slabFilledTo.length * 2);
    }

    byte[] getHashSlab() {
      return new byte[HASH_SLAB_BYTES];
    }

    void addHash(long hash, long entry) {
      int hash0 = (int) hash & statusMask;
      long field = (hash >> 56) & 0x7f;
      byte statusByte = (byte) field;
      TableShard shard0 = shards[0];
      byte[][] status0 = shard0.status;
      int statusMask0 = shard0.statusMask;
      byte[][] entries0 = shard0.entries;
      byte[][] slabs0 = shard0.slabs;
      for (; ; ) {
        long hits0;
        hits0 =
            UncheckedByteArrays.getLongUnchecked(
                status0[hash0 >> (16 - 3)], ((hash0) * 8) & 0xffff);
        long free = hits0 & 0x8080808080808080L;
        if (free != 0) {
          int pos = Long.numberOfTrailingZeros(free) >> 3;
          long newStatus = hits0 ^ (long) (statusByte | 0x80) << (pos * 8);

          ByteArrays.setLong(status0[hash0 >> (16 - 3)], ((hash0) * 8) & 0xffff, newStatus);
          ByteArrays.setLong(
              entries0[(hash0 * 8 + pos) >> (16 - 3)], ((hash0 * 8 + pos) * 8) & 0xffff, entry);
          break;
        }
        hash0 = (hash0 + 1) & statusMask;
      }
      numEntries++;
    }

    long getSize() {
      return numSlabs * ROW_SLAB_BYTES + entries.length * HASH_SLAB_BYTES;
    }
  }

  public void groupByInput(Block[] blocks) {
    int totalInput = InitializeCandidates(blocks);
    int firstKeyOffset;
    if (firstLongKey != -1) {
      firstKeyOffset = columns.get(firstLongKey).offset;
    } else {
      firstKeyOffset = columns.get(0).offset;
    }
    for (int window = 0; window < totalInput; window += PROBE_WINDOW) {
      int windowEnd = Math.min(totalInput, window + PROBE_WINDOW);
      for (int i = window; i < windowEnd; i++) {
        candidates[i - window] = i;
      }
      numCandidates = windowEnd - window;
      while (numCandidates > 0) {
        TableShard shard0 = shards[0];
        byte[][] status0 = shard0.status;
        int statusMask0 = shard0.statusMask;
        byte[][] entries0 = shard0.entries;
        byte[][] slabs0 = shard0.slabs;
        numHitCandidates = 0;
        int currentProbe = 0;
        long tempHash;
        long encodedPayload;
        long entry0 = -1;
        long field0;
        long empty0;
        long hits0;
        int hash0;
        int row0;
        byte[] payload0Bytes = null;
        int payload0Offset = -1;
        ;;
        long entry1 = -1;
        long field1;
        long empty1;
        long hits1;
        int hash1;
        int row1;
        byte[] payload1Bytes = null;
        int payload1Offset = -1;
        ;;
        long entry2 = -1;
        long field2;
        long empty2;
        long hits2;
        int hash2;
        int row2;
        byte[] payload2Bytes = null;
        int payload2Offset = -1;
        ;;
        long entry3 = -1;
        long field3;
        long empty3;
        long hits3;
        int hash3;
        int row3;
        byte[] payload3Bytes = null;
        int payload3Offset = -1;
        ;;
        if (unroll) {
          for (; currentProbe + 4 < numCandidates; currentProbe += 4) {
            row0 = candidates[currentProbe + 0];
            tempHash = hashes[row0];
            hash0 = (int) tempHash & statusMask0;
            field0 = (tempHash >> 56) & 0x7f;
            hits0 =
                UncheckedByteArrays.getLongUnchecked(
                    status0[hash0 >> (16 - 3)], ((hash0) * 8) & 0xffff);
            field0 |= field0 << 8;
            field0 |= field0 << 16;
            field0 |= field0 << 32;
            ;
            row1 = candidates[currentProbe + 1];
            tempHash = hashes[row1];
            hash1 = (int) tempHash & statusMask0;
            field1 = (tempHash >> 56) & 0x7f;
            hits1 =
                UncheckedByteArrays.getLongUnchecked(
                    status0[hash1 >> (16 - 3)], ((hash1) * 8) & 0xffff);
            field1 |= field1 << 8;
            field1 |= field1 << 16;
            field1 |= field1 << 32;
            ;
            row2 = candidates[currentProbe + 2];
            tempHash = hashes[row2];
            hash2 = (int) tempHash & statusMask0;
            field2 = (tempHash >> 56) & 0x7f;
            hits2 =
                UncheckedByteArrays.getLongUnchecked(
                    status0[hash2 >> (16 - 3)], ((hash2) * 8) & 0xffff);
            field2 |= field2 << 8;
            field2 |= field2 << 16;
            field2 |= field2 << 32;
            ;
            row3 = candidates[currentProbe + 3];
            tempHash = hashes[row3];
            hash3 = (int) tempHash & statusMask0;
            field3 = (tempHash >> 56) & 0x7f;
            hits3 =
                UncheckedByteArrays.getLongUnchecked(
                    status0[hash3 >> (16 - 3)], ((hash3) * 8) & 0xffff);
            field3 |= field3 << 8;
            field3 |= field3 << 16;
            field3 |= field3 << 32;
            ;
            empty0 = hits0 & 0x8080808080808080L;
            hits0 ^= field0;
            hits0 -= 0x0101010101010101L;
            hits0 &= 0x8080808080808080L ^ empty0;
            if (hits0 != 0) {
              int pos = Long.numberOfTrailingZeros(hits0) >> 3;
              encodedPayload =
                  UncheckedByteArrays.getLongUnchecked(
                      entries0[(hash0 * 8 + pos) >> (16 - 3)], ((hash0 * 8 + pos) * 8) & 0xffff);
              {
                payload0Bytes = slabs0[(int) (encodedPayload >> 24)];
                payload0Offset = (int) encodedPayload & 0xffffff;
              }
              ;
            }
            ;
            empty1 = hits1 & 0x8080808080808080L;
            hits1 ^= field1;
            hits1 -= 0x0101010101010101L;
            hits1 &= 0x8080808080808080L ^ empty1;
            if (hits1 != 0) {
              int pos = Long.numberOfTrailingZeros(hits1) >> 3;
              encodedPayload =
                  UncheckedByteArrays.getLongUnchecked(
                      entries0[(hash1 * 8 + pos) >> (16 - 3)], ((hash1 * 8 + pos) * 8) & 0xffff);
              {
                payload1Bytes = slabs0[(int) (encodedPayload >> 24)];
                payload1Offset = (int) encodedPayload & 0xffffff;
              }
              ;
            }
            ;
            empty2 = hits2 & 0x8080808080808080L;
            hits2 ^= field2;
            hits2 -= 0x0101010101010101L;
            hits2 &= 0x8080808080808080L ^ empty2;
            if (hits2 != 0) {
              int pos = Long.numberOfTrailingZeros(hits2) >> 3;
              encodedPayload =
                  UncheckedByteArrays.getLongUnchecked(
                      entries0[(hash2 * 8 + pos) >> (16 - 3)], ((hash2 * 8 + pos) * 8) & 0xffff);
              {
                payload2Bytes = slabs0[(int) (encodedPayload >> 24)];
                payload2Offset = (int) encodedPayload & 0xffffff;
              }
              ;
            }
            ;
            empty3 = hits3 & 0x8080808080808080L;
            hits3 ^= field3;
            hits3 -= 0x0101010101010101L;
            hits3 &= 0x8080808080808080L ^ empty3;
            if (hits3 != 0) {
              int pos = Long.numberOfTrailingZeros(hits3) >> 3;
              encodedPayload =
                  UncheckedByteArrays.getLongUnchecked(
                      entries0[(hash3 * 8 + pos) >> (16 - 3)], ((hash3 * 8 + pos) * 8) & 0xffff);
              {
                payload3Bytes = slabs0[(int) (encodedPayload >> 24)];
                payload3Offset = (int) encodedPayload & 0xffffff;
              }
              ;
            }
            ;
            if (hits0 != 0) {
              hits0 &= hits0 - 1;
              firstWord[numHitCandidates] =
                  UncheckedByteArrays.getLongUnchecked(
                      payload0Bytes, payload0Offset + firstKeyOffset);
              {
                hitCandidateBytes[numHitCandidates] = payload0Bytes;
                hitCandidateOffset[numHitCandidates] = payload0Offset;
                hitCandidatePositions[numHitCandidates++] = row0;
              }
              ;
            }
            bucketLoop0:
            for (; ; ) {
              while (hits0 != 0) {
                int pos = Long.numberOfTrailingZeros(hits0) >> 3;
                encodedPayload =
                    UncheckedByteArrays.getLongUnchecked(
                        entries0[(hash0 * 8 + pos) >> (16 - 3)], ((hash0 * 8 + pos) * 8) & 0xffff);
                {
                  payload0Bytes = slabs0[(int) (encodedPayload >> 24)];
                  payload0Offset = (int) encodedPayload & 0xffffff;
                }
                ;
                firstWord[numHitCandidates] =
                    UncheckedByteArrays.getLongUnchecked(
                        payload0Bytes, payload0Offset + firstKeyOffset);
                if (numHitCandidates > hitCandidatesSize + 256) {
                  growHitCandidates();
                }
                {
                  hitCandidateBytes[numHitCandidates] = payload0Bytes;
                  hitCandidateOffset[numHitCandidates] = payload0Offset;
                  hitCandidatePositions[numHitCandidates++] = row0;
                }
                ;
                hits0 &= hits0 - 1;
              }
              if (empty0 != 0) break;
              hash0 = (hash0 + 1) & statusMask0;
              hits0 =
                  UncheckedByteArrays.getLongUnchecked(
                      status0[hash0 >> (16 - 3)], ((hash0) * 8) & 0xffff);
              empty0 = hits0 & 0x8080808080808080L;
              hits0 ^= field0;
              hits0 -= 0x0101010101010101L;
              hits0 &= 0x8080808080808080L ^ empty0;
            }
            ;
            if (hits1 != 0) {
              hits1 &= hits1 - 1;
              firstWord[numHitCandidates] =
                  UncheckedByteArrays.getLongUnchecked(
                      payload1Bytes, payload1Offset + firstKeyOffset);
              {
                hitCandidateBytes[numHitCandidates] = payload1Bytes;
                hitCandidateOffset[numHitCandidates] = payload1Offset;
                hitCandidatePositions[numHitCandidates++] = row1;
              }
              ;
            }
            bucketLoop1:
            for (; ; ) {
              while (hits1 != 0) {
                int pos = Long.numberOfTrailingZeros(hits1) >> 3;
                encodedPayload =
                    UncheckedByteArrays.getLongUnchecked(
                        entries0[(hash1 * 8 + pos) >> (16 - 3)], ((hash1 * 8 + pos) * 8) & 0xffff);
                {
                  payload1Bytes = slabs0[(int) (encodedPayload >> 24)];
                  payload1Offset = (int) encodedPayload & 0xffffff;
                }
                ;
                firstWord[numHitCandidates] =
                    UncheckedByteArrays.getLongUnchecked(
                        payload1Bytes, payload1Offset + firstKeyOffset);
                if (numHitCandidates > hitCandidatesSize + 256) {
                  growHitCandidates();
                }
                {
                  hitCandidateBytes[numHitCandidates] = payload1Bytes;
                  hitCandidateOffset[numHitCandidates] = payload1Offset;
                  hitCandidatePositions[numHitCandidates++] = row1;
                }
                ;
                hits1 &= hits1 - 1;
              }
              if (empty1 != 0) break;
              hash1 = (hash1 + 1) & statusMask0;
              hits1 =
                  UncheckedByteArrays.getLongUnchecked(
                      status0[hash1 >> (16 - 3)], ((hash1) * 8) & 0xffff);
              empty1 = hits1 & 0x8080808080808080L;
              hits1 ^= field1;
              hits1 -= 0x0101010101010101L;
              hits1 &= 0x8080808080808080L ^ empty1;
            }
            ;
            if (hits2 != 0) {
              hits2 &= hits2 - 1;
              firstWord[numHitCandidates] =
                  UncheckedByteArrays.getLongUnchecked(
                      payload2Bytes, payload2Offset + firstKeyOffset);
              {
                hitCandidateBytes[numHitCandidates] = payload2Bytes;
                hitCandidateOffset[numHitCandidates] = payload2Offset;
                hitCandidatePositions[numHitCandidates++] = row2;
              }
              ;
            }
            bucketLoop2:
            for (; ; ) {
              while (hits2 != 0) {
                int pos = Long.numberOfTrailingZeros(hits2) >> 3;
                encodedPayload =
                    UncheckedByteArrays.getLongUnchecked(
                        entries0[(hash2 * 8 + pos) >> (16 - 3)], ((hash2 * 8 + pos) * 8) & 0xffff);
                {
                  payload2Bytes = slabs0[(int) (encodedPayload >> 24)];
                  payload2Offset = (int) encodedPayload & 0xffffff;
                }
                ;
                firstWord[numHitCandidates] =
                    UncheckedByteArrays.getLongUnchecked(
                        payload2Bytes, payload2Offset + firstKeyOffset);
                if (numHitCandidates > hitCandidatesSize + 256) {
                  growHitCandidates();
                }
                {
                  hitCandidateBytes[numHitCandidates] = payload2Bytes;
                  hitCandidateOffset[numHitCandidates] = payload2Offset;
                  hitCandidatePositions[numHitCandidates++] = row2;
                }
                ;
                hits2 &= hits2 - 1;
              }
              if (empty2 != 0) break;
              hash2 = (hash2 + 1) & statusMask0;
              hits2 =
                  UncheckedByteArrays.getLongUnchecked(
                      status0[hash2 >> (16 - 3)], ((hash2) * 8) & 0xffff);
              empty2 = hits2 & 0x8080808080808080L;
              hits2 ^= field2;
              hits2 -= 0x0101010101010101L;
              hits2 &= 0x8080808080808080L ^ empty2;
            }
            ;
            if (hits3 != 0) {
              hits3 &= hits3 - 1;
              firstWord[numHitCandidates] =
                  UncheckedByteArrays.getLongUnchecked(
                      payload3Bytes, payload3Offset + firstKeyOffset);
              {
                hitCandidateBytes[numHitCandidates] = payload3Bytes;
                hitCandidateOffset[numHitCandidates] = payload3Offset;
                hitCandidatePositions[numHitCandidates++] = row3;
              }
              ;
            }
            bucketLoop3:
            for (; ; ) {
              while (hits3 != 0) {
                int pos = Long.numberOfTrailingZeros(hits3) >> 3;
                encodedPayload =
                    UncheckedByteArrays.getLongUnchecked(
                        entries0[(hash3 * 8 + pos) >> (16 - 3)], ((hash3 * 8 + pos) * 8) & 0xffff);
                {
                  payload3Bytes = slabs0[(int) (encodedPayload >> 24)];
                  payload3Offset = (int) encodedPayload & 0xffffff;
                }
                ;
                firstWord[numHitCandidates] =
                    UncheckedByteArrays.getLongUnchecked(
                        payload3Bytes, payload3Offset + firstKeyOffset);
                if (numHitCandidates > hitCandidatesSize + 256) {
                  growHitCandidates();
                }
                {
                  hitCandidateBytes[numHitCandidates] = payload3Bytes;
                  hitCandidateOffset[numHitCandidates] = payload3Offset;
                  hitCandidatePositions[numHitCandidates++] = row3;
                }
                ;
                hits3 &= hits3 - 1;
              }
              if (empty3 != 0) break;
              hash3 = (hash3 + 1) & statusMask0;
              hits3 =
                  UncheckedByteArrays.getLongUnchecked(
                      status0[hash3 >> (16 - 3)], ((hash3) * 8) & 0xffff);
              empty3 = hits3 & 0x8080808080808080L;
              hits3 ^= field3;
              hits3 -= 0x0101010101010101L;
              hits3 &= 0x8080808080808080L ^ empty3;
            }
            ;
          }
        }
        for (; currentProbe < numCandidates; currentProbe++) {
          row0 = candidates[currentProbe + 0];
          tempHash = hashes[row0];
          hash0 = (int) tempHash & statusMask0;
          field0 = (tempHash >> 56) & 0x7f;
          hits0 =
              UncheckedByteArrays.getLongUnchecked(
                  status0[hash0 >> (16 - 3)], ((hash0) * 8) & 0xffff);
          field0 |= field0 << 8;
          field0 |= field0 << 16;
          field0 |= field0 << 32;
          ;
          empty0 = hits0 & 0x8080808080808080L;
          hits0 ^= field0;
          hits0 -= 0x0101010101010101L;
          hits0 &= 0x8080808080808080L ^ empty0;
          if (hits0 != 0) {
            int pos = Long.numberOfTrailingZeros(hits0) >> 3;
            encodedPayload =
                UncheckedByteArrays.getLongUnchecked(
                    entries0[(hash0 * 8 + pos) >> (16 - 3)], ((hash0 * 8 + pos) * 8) & 0xffff);
            {
              payload0Bytes = slabs0[(int) (encodedPayload >> 24)];
              payload0Offset = (int) encodedPayload & 0xffffff;
            }
            ;
          }
          ;
          if (hits0 != 0) {
            hits0 &= hits0 - 1;
            firstWord[numHitCandidates] =
                UncheckedByteArrays.getLongUnchecked(
                    payload0Bytes, payload0Offset + firstKeyOffset);
            {
              hitCandidateBytes[numHitCandidates] = payload0Bytes;
              hitCandidateOffset[numHitCandidates] = payload0Offset;
              hitCandidatePositions[numHitCandidates++] = row0;
            }
            ;
          }
          bucketLoop0:
          for (; ; ) {
            while (hits0 != 0) {
              int pos = Long.numberOfTrailingZeros(hits0) >> 3;
              encodedPayload =
                  UncheckedByteArrays.getLongUnchecked(
                      entries0[(hash0 * 8 + pos) >> (16 - 3)], ((hash0 * 8 + pos) * 8) & 0xffff);
              {
                payload0Bytes = slabs0[(int) (encodedPayload >> 24)];
                payload0Offset = (int) encodedPayload & 0xffffff;
              }
              ;
              firstWord[numHitCandidates] =
                  UncheckedByteArrays.getLongUnchecked(
                      payload0Bytes, payload0Offset + firstKeyOffset);
              if (numHitCandidates > hitCandidatesSize + 256) {
                growHitCandidates();
              }
              {
                hitCandidateBytes[numHitCandidates] = payload0Bytes;
                hitCandidateOffset[numHitCandidates] = payload0Offset;
                hitCandidatePositions[numHitCandidates++] = row0;
              }
              ;
              hits0 &= hits0 - 1;
            }
            if (empty0 != 0) break;
            hash0 = (hash0 + 1) & statusMask0;
            hits0 =
                UncheckedByteArrays.getLongUnchecked(
                    status0[hash0 >> (16 - 3)], ((hash0) * 8) & 0xffff);
            empty0 = hits0 & 0x8080808080808080L;
            hits0 ^= field0;
            hits0 -= 0x0101010101010101L;
            hits0 &= 0x8080808080808080L ^ empty0;
          }
          ;
        }
        compareKeys(blocks);
        insertMisses(blocks);
        updateGroups(blocks);
        System.arraycopy(toRecheck, 0, candidates, 0, numToRecheck);
        numCandidates = numToRecheck;
      }
    }
  }

  private class OutputState {
    int outputBatch = 1024;
    byte[][] rowsBytes;
    int[] rowsOffset;;
    int numOutput;
    int currentShard;
    int currentSlab;
    int currentOffset;

    OutputState() {
      {
        rowsBytes = new byte[outputBatch][];
        rowsOffset = new int[outputBatch];
      }
      ;
    }

    void reset() {
      currentShard = 0;
      currentSlab = 0;
      currentOffset = 0;
      numOutput = 0;
    }

    Block getBlock(int from, int to, Column column) {
      int offset = column.offset;
      switch (column.typeCode) {
        case LONG_TYPE:
        case DOUBLE_TYPE:
          {
            long[] array = new long[to - from];
            byte[] ptrBytes = null;
            int ptrOffset = -1;
            ;
            for (int i = from; i < to; i++) {
              {
                ptrBytes = rowsBytes[i];
                ptrOffset = rowsOffset[i];
              }
              ;
              array[i - from] = UncheckedByteArrays.getLongUnchecked(ptrBytes, ptrOffset + offset);
            }
            return new LongArrayBlock(to - from, Optional.empty(), array);
          }
        default:
          {
            throw new UnsupportedOperationException("Column type not supported");
          }
      }
    }

    void nextBatch() {
      TableShard shard = shards[currentShard];
      byte[] ptrBytes = null;
      int ptrOffset = -1;
      ;
      numOutput = 0;
      while (true) {
        ptrBytes = shard.slabs[currentSlab];
        ptrOffset = currentOffset;
        rowsBytes[numOutput] = ptrBytes;
        rowsOffset[numOutput] = ptrOffset;
        numOutput++;
        currentOffset += fixedRowSize;
        if (currentOffset >= shard.slabFilledTo[currentSlab]) {
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

    Page getOutput() {
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
      Block[] blocks = new Block[outputColumns.size()];
      for (int i = 0; i < outputColumns.size(); i++) {
        Column column = outputColumns.get(i);
        blocks[i] = getBlock(0, numOutput, column);
      }
      return new Page(numOutput, blocks);
    }

    boolean outputDone() {
      return currentShard >= shards.length;
    }
  }

  public Page getOutput() {
    if (outputState == null) {
      outputState = new OutputState();
    }
    return outputState.getOutput();
  }

  private void compareKeys(Block[] blocks) {
    Arrays.fill(maskWords, 0, roundUp(numHitCandidates, 64) / 64, -1);
    if (firstLongKey != -1) {
      Block probeBlock = blocks[groupByChannels[firstLongKey]];
      if (probeBlock instanceof LongArrayBlock) {
        LongArrayBlock longBlock = (LongArrayBlock) probeBlock;
        int base = longBlock.getOffsetBase();
        for (int windowBase = 0; windowBase < numHitCandidates; windowBase += 64) {
          int windowEnd = Math.min(windowBase + 64, numHitCandidates);
          long maskWord = maskWords[windowBase / 64];
          long mask = 1;
          for (int i = windowBase; i < windowEnd; i++) {
            if (firstWord[i] != longBlock.getLongUnchecked(hitCandidatePositions[i] + base)) {
              maskWord &= ~mask;
            }
            mask = mask << 1;
          }
          maskWords[windowBase / 64] = maskWord;
        }
      } else {
        throw new UnsupportedOperationException("Only LongArrayBlocks for fast aggregation");
      }
    }

    numHits = 0;
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
      } else {
        verify(false, "candidates out of order");
      }
    }
  }

  private void insertMisses(Block[] blocks) {
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
      long bloomMask =
          ((1L << (63 & ((hash) >> 32)))
              | (1L << (63 & ((hash) >> 38)))
              | (1L << (63 & ((hash) >> 44)))
              | (1L << (63 & ((hash) >> 50))));
      int bloomIndex = (int) hash & insertBloomSizeMask;
      if ((insertBloom[bloomIndex] & bloomMask) == bloomMask) {
        toRecheck[numToRecheck++] = row;
        continue;
      }
      insertBloom[bloomIndex] |= bloomMask;
      byte[] newRowBytes = null;
      int newRowOffset = -1;
      ;
      long encodedNewRow;
      shard.newRow(fixedRowSize);
      {
        newRowBytes = shard.slabs[shard.lastAllocSlabIndex];
        newRowOffset = shard.lastAllocOffset;
        encodedNewRow = shard.lastAllocOffset + ((long) shard.lastAllocSlabIndex << 24);
      }
      ;
      hitRows[numHits] = numHitCandidates;
      actualHits[numHits++] = row;
      {
        hitCandidateBytes[numHitCandidates] = newRowBytes;
        hitCandidateOffset[numHitCandidates] = newRowOffset;
        hitCandidatePositions[numHitCandidates++] = row;
      }
      ;
      shard.addHash(hash, encodedNewRow);
    }
    initializeNewRows(initialCandidates, numHitCandidates, blocks);
  }

  private void initializeNewRows(int from, int to, Block[] blocks) {
    TableShard shard0 = shards[0];
    byte[][] status0 = shard0.status;
    int statusMask0 = shard0.statusMask;
    byte[][] entries0 = shard0.entries;
    byte[][] slabs0 = shard0.slabs;
    byte[] ptrBytes = null;
    int ptrOffset = -1;
    ;
    for (int columnIndex = 0; columnIndex < keys.size(); columnIndex++) {
      int offset = columns.get(columnIndex).offset;
      Block block = blocks[groupByChannels[columnIndex]];
      int base = block.getOffsetBase();
      for (int i = from; i < to; i++) {
        int row = hitCandidatePositions[i];
        {
          ptrBytes = hitCandidateBytes[i];
          ptrOffset = hitCandidateOffset[i];
        }
        ;
        ByteArrays.setLong(ptrBytes, ptrOffset + offset, block.getLongUnchecked(row + base));
      }
    }
    for (int aggregationIndex = 0; aggregationIndex < aggregations.size(); aggregationIndex++) {
      Aggregation aggregation = aggregations.get(aggregationIndex);
      int offset = aggregation.columns.get(0).offset;
      for (int i = from; i < to; i++) {
        {
          ptrBytes = hitCandidateBytes[i];
          ptrOffset = hitCandidateOffset[i];
        }
        ;
        ByteArrays.setLong(ptrBytes, ptrOffset + offset, 0);
      }
    }
  }

  private int InitializeCandidates(Block[] blocks) {
    if (shards == null) {
      shards = new TableShard[1];
      shards[0] = new TableShard(8192);
    }
    numCandidates = blocks[0].getPositionCount();
    candidates = ensureCapacity(candidates, PROBE_WINDOW);
    misses = ensureCapacity(misses, PROBE_WINDOW);
    hashes = ensureCapacity(hashes, numCandidates);
    if (hashChannel.isPresent()) {
      Block hashBlock = blocks[hashChannel.get()];
      int base = hashBlock.getOffsetBase();
      for (int i = 0; i < numCandidates; i++) {
        hashes[i] = hashBlock.getLongUnchecked(i + base);
      }
    } else {
      hashKeys(blocks, hashes, numCandidates, true);
    }
    if (hitCandidatesSize == 0) {
      actualHits = new int[PROBE_WINDOW];
      hitCandidatesSize = PROBE_WINDOW * 3;
      {
        hitCandidateBytes = new byte[hitCandidatesSize][];
        hitCandidateOffset = new int[hitCandidatesSize];
      }
      ;
      hitCandidatePositions = new int[hitCandidatesSize];
      firstWord = new long[hitCandidatesSize];
      maskWords = new long[(hitCandidatesSize + 64) / 64];
    }
    return numCandidates;
  }

  private void hashKeys(Block[] blocks, long[] hashes, int numInput, boolean nullsAllowed) {
    for (int keyIndex = 0; keyIndex < keys.size(); keyIndex++) {
      Block block = blocks[groupByChannels[keyIndex]];
      if (block instanceof LongArrayBlock) {
        LongArrayBlock longBlock = (LongArrayBlock) block;
        int base = longBlock.getOffsetBase();
        for (int i = 0; i < numInput; i++) {
          long hash = AbstractLongType.hash(longBlock.getLongUnchecked(i + base));
          hashes[i] = keyIndex == 0 ? hash : hashMix(hashes[i], hash);
        }
      } else {
        throw new UnsupportedOperationException();
      }
    }
  }

  static long hashMix(long previousHash, long hash) {
    return 31 * previousHash + hash;
  }

  private void growHitCandidates() {
    hitCandidatesSize *= 2;
    {
      hitCandidateBytes = Arrays.copyOf(hitCandidateBytes, hitCandidatesSize);
      hitCandidateOffset = Arrays.copyOf(hitCandidateOffset, hitCandidatesSize);
    }
    ;
    hitCandidatePositions = Arrays.copyOf(hitCandidatePositions, hitCandidatesSize);
    firstWord = Arrays.copyOf(firstWord, hitCandidatesSize);
    maskWords = new long[(hitCandidatesSize + 64) / 64];
  }

  void compareColumn(
      Column column,
      Block block,
      byte[][] hitRowsBytes,
      int[] hitRowsOffset,
      int numHits,
      int[] candidateInputs,
      long[] maskWords) {
    int blockBase = block.getOffsetBase();
    int columnOffset = column.offset;
    byte[] rowBytes = null;
    int rowOffset = -1;
    ;
    for (int base = 0; base < numHits; base += 64) {
      int end = Math.min(numHits, base + 64);
      long maskWord = maskWords[base / 64];
      long mask = 1;
      for (int i = base; i < end; i++) {
        if ((maskWord & mask) != 0) {
          {
            rowBytes = hitCandidateBytes[i];
            rowOffset = hitCandidateOffset[i];
          }
          ;
          if (UncheckedByteArrays.getLongUnchecked(rowBytes, rowOffset + columnOffset)
              != block.getLongUnchecked(candidateInputs[i] + blockBase)) {
            maskWord &= ~mask;
          }
          mask = mask << 1;
        }
        maskWords[base / 64] = maskWord;
      }
    }
  }

  void updateGroups(Block[] blocks) {
    for (Aggregation aggregation : aggregations) {
      aggregation.addInput(
          hitCandidateBytes, hitCandidateOffset, hitRows, actualHits, numHits, blocks);
    }
  }

  public int hashProbeInput(Block[] keys, int[] candidates, byte[][] hitsBytes, int[] hitsOffset) {
    return 0;
  }

  public long getEstimatedSize() {
    long size = 0;
    for (TableShard shard : shards) {
      if (shard != null) {
        size += shard.getSize();
      }
    }
    return size;
  }

  public long getGroupCount() {
    long size = 0;
    for (TableShard shard : shards) {
      if (shard != null) {
        size += shard.numEntries;
      }
    }
    return size;
  }

  public int getCapacity() {
    int size = 0;
    for (TableShard shard : shards) {
      if (shard != null) {
        size += shard.capacity;
      }
    }
    return size;
  }

  public List<Type> getTypes() {
    return keys;
  }

  public void close() {
    for (TableShard shard : shards) {
      shard.close();
    }
    shards = null;
  }

  static int getCodeForType(Type type) {
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

  static int roundUp(int number, int upTo) {
    return ((number + upTo - 1) / upTo) * upTo;
  }

  private static int nextPowerOf2(int number) {
    return Integer.bitCount(number) == 1 ? number : Integer.highestOneBit(number << 1);
  }
}
