


#define MHASH_M  0xc6a4a7935bd1e995L
#define MHASH_R 47

#define MHASH_STEP_1(h, data)                   \
{ \
  long __k = data; \
      __k *= MHASH_M;  \
      __k ^= __k >> MHASH_R;  \
      h = __k * MHASH_M; \
    }

    #define MHASH_STEP(h, data) \
{ \
  long __k = data; \
      __k *= MHASH_M;  \
      __k ^= __k >> MHASH_R;  \
      __k *= MHASH_M;  \
      h ^= __k; \
      h *= MHASH_M; \
    }

#define BF_BIT_1(h) (63 & ((h) >> 32))
#define BF_BIT_2(h) (63 & ((h) >> 38))
#define BF_BIT_3(h) (63 & ((h) >> 44))
#define BF_BIT_4(h) (63 & ((h) >> 50))
    #define BF_WORD(h, size) (int)((h & 0x7fffffff) % size)
#define BF_MASK(h) ((1L << BF_BIT_1 (h)) | (1L << BF_BIT_2 (h)) | (1L << BF_BIT_3 (h)) | (1L << BF_BIT_4 (h)))




#ifdef UNSAFE

    #define ALLOC_LONG_ARRAY(size) unsafe.allocateMemory(size * 8) 
        #define FREE_LONG_ARRAY(a) unsafe.freeMemory(a)
            #define DECL_ARRAY(prefix)
        #define LONG_ARRAY long
    #define ARRAY_PREGET(prefix, array, index)
        #define ARRAY_GET(prefix, array, index) unsafe.getLong((array) + 8 * (index))
        #define ARRAY_SET(prefix, array, index, value) unsafe.putLong((array) + 8 * (index), value)

#define LABEL "With offheap"

#define DECLTABLE(table_in)                     \
  HashShard table = table_in;            \
  int statusMask = table.statusMask;            \
  long[] slabs = table.slabs;

#define DECLTABLE_N(table_in, sub)              \
  HashShard table##sub = table_in;       \
  int statusMask##sub = table##sub.statusMask;  \
  long[] slabs##sub = table##sub.slabs;

      
#define DECLGET(prefix) long prefix##ptr

#define PREGET_N(prefix, l, sub)                \
  prefix##ptr = l


#define GETL(prefix, field)                     \
  unsafe.getLong(prefix##ptr + field)

#define SETL(prefix, field, v)                  \
  unsafe.putLong(prefix##ptr + field, v)


#elif FLAT_BYTE_ARRAY

#define LABEL "With flat byte arrays"

#define ALLOC_LONG_ARRAY(size) new long[size]
            #define FREE_LONG_ARRAY(a)
            #define DECL_ARRAY(prefix)
        #define LONG_ARRAY long[]
    #define ARRAY_PREGET(prefix, array, index)
    #define ARRAY_GET(prefix, array, index) array[index]
            #define ARRAY_SET(prefix, array, index, value) array[index] = value

#define DECL_STATUS(name) \
  byte[][] status##name;

#define LOAD_STATUS(name, index) \
  UncheckedByteArrays.getLong(status##name, index)

#define SET_STATUS(name, index, byteIndex, statusByte)                     \
  UncheckedByteArrays.putByte(status##name, index * 8 + byteIndex, statusByte)


#define DECL_ROWS(name) \
  byte[] rows##name;    

#define ALLOC_ROWS(size) \
      new byte[size * 5 + 3]

#define GET_ROW(name, index) \
  (UncheckedByteArrays.getLong(rows##name, index * 5) & 0xffffffffffL)

#define SET_ROW(name, index, pointer) \
  { UncheckedByteArrays.putInt(name, index * 5, (int) pointer); UncheckedByteArrays.putByte(index * 5 + 4, (byte) (pointer >> 32)); }

#else 
#define LABEL "With byte arrays"

#define UNIT_SHIFT 16
#define UNIT_MASK 0xffff
#define ENTRY_MASK 0xffffff
#define ENTRY_SHIFT 24

#define DECL_STATUS(name)                       \
    byte[][] name

#define DECL_ENTRIES(entries) \
  byte[][] entries

#define DECL_SLABS(slabs) \
  byte[][] slabs

#define USE_TABLE(sub, partitionNumber) \
  TableShard shard##sub = shards[partitionNumber]; \
  byte[][] status##sub = shard##sub.status;      \
  int statusMask##sub = shard##sub.statusMask; \
  byte[][] entries##sub = shard##sub.entries; \
  byte[][] slabs##sub = shard##sub.slabs

#define DECL_PTR(name) \
  byte[] name##Bytes = null; \
  int name##Offset = -1;
  

// index is the index of a 8 byte word
#define LOAD_STATUS(dest, tablesub, index)                          \
        dest = UncheckedByteArrays.getLongUnchecked(status##tablesub[index >> (UNIT_SHIFT - 3)], ((index) * 8) & UNIT_MASK)

#define LOAD_ENTRY(dest, tablesub, index) \
  dest = UncheckedByteArrays.getLongUnchecked(entries##tablesub[(index) >> (UNIT_SHIFT - 3)], ((index) * 8)& UNIT_MASK)

#define STORE_STATUS(tablesub, index, newValue)                     \
  ByteArrays.setLong(status##tablesub[index >> (UNIT_SHIFT - 3)], ((index) * 8) & UNIT_MASK, newValue)

#define STORE_ENTRY(tablesub, index, newValue)                                   \
  ByteArrays.setLong(entries##tablesub[(index) >> (UNIT_SHIFT - 3)], ((index) * 8)& UNIT_MASK, newValue)


#define DECODE_ENTRY(ptr, tablesub, entry)                     \
  { ptr##Bytes = slabs##tablesub[(int) (entry >> ENTRY_SHIFT)]; \
    ptr##Offset = (int)entry & ENTRY_MASK; }


#define GETLONG(ptr, off) \
  UncheckedByteArrays.getLongUnchecked(ptr##Bytes, ptr##Offset + off)

#define SETLONG(ptr, off, value)                              \
  ByteArrays.setLong(ptr##Bytes, ptr##Offset + off, value)


#define DECL_ROWS(rows) \
  byte[][] rows##Bytes; \
  int[] rows##Offset;
#define ALLOC_ROWS(rows, size) \
  { rows##Bytes = new byte[size][];              \
    rows##Offset = new int[size]; }

#define GROW_ROWS(rows, size) \
  { rows##Bytes = Arrays.copyOf(rows##Bytes, size); \
    rows##Offset = Arrays.copyOf(rows##Offset, size); }

#define TAKE_ROWS(rows) byte[][] rows##Bytes, int[] rows##Offset

#define PASS_ROWS(rows) rows##Bytes, rows##Offset

#define LOAD_PTR(ptr, rows, index) \
  { ptr##Bytes = rows##Bytes[index]; \
    ptr##Offset = rows##Offset[index]; }

#define ADD_ROW(rows, row, numRows, ptr) \
  { rows##Bytes[numRows] = ptr##Bytes; \
    rows##Offset[numRows] = ptr##Offset; \
    rows##Positions[numRows++] = row; }

#define GET_NEW_ROW(ptr, encodedPtr, shard)          \
  { ptr##Bytes = shard.slabs[shard.lastAllocSlabIndex]; \
    ptr##Offset = shard.lastAllocOffset;                                \
    encodedPtr = shard.lastAllocOffset + ((long) shard.lastAllocSlabIndex << ENTRY_SHIFT); }


#define ENCODE_PTR(entryIndex, offset) \
  (((long) (entryIndex) << ENTRY_SHIFT) + (offset))

#endif




#define DECL_PROBE(sub)                   \
		    long entry##sub = -1; \
		    long field##sub;		\
    long empty##sub; \
	    long hits##sub; \
	    int hash##sub; \
	    int row##sub; \
	DECL_PTR(payload##sub); \


#define PRE_PROBE(sub, tablesub)                                     \
		row##sub = candidates[currentProbe + sub]; \
        tempHash = hashes[row##sub]; \
	hash##sub = (int)tempHash & statusMask##tablesub;	   \
	    field##sub = (tempHash >> 56) & 0x7f; \
            LOAD_STATUS(hits##sub, tablesub, hash##sub);     \
	    field##sub |= field##sub << 8; \
	    field##sub |= field##sub << 16; \
	    field##sub |= field##sub << 32; 
	

#define FIRST_PROBE(sub, tablesub)                                  \
            empty##sub = hits##sub & 0x8080808080808080L; \
	    hits##sub ^= field##sub;  \
	hits##sub -= 0x0101010101010101L; \
	hits##sub &= 0x8080808080808080L ^ empty##sub; \
	if (hits##sub != 0) { \
	    int pos = Long.numberOfTrailingZeros(hits##sub) >> 3; \
            LOAD_ENTRY(encodedPayload, tablesub, hash##sub * 8 + pos); \
            DECODE_ENTRY(payload##sub, tablesub, encodedPayload);             \
}


#define FULL_PROBE(sub, tablesub)  \
  if (hits##sub != 0) { \
    hits##sub &= hits##sub - 1;                                         \
    firstWord[numHitCandidates] = GETLONG(payload##sub, firstKeyOffset); \
    ADD_ROW(hitCandidate, row##sub, numHitCandidates, payload##sub);    \
  }                                                                     \
bucketLoop##sub:                 \
	for (;;) {		 \
	while (hits##sub != 0) { \
	    int pos = Long.numberOfTrailingZeros(hits##sub) >> 3; \
            LOAD_ENTRY(encodedPayload, tablesub, hash##sub * 8 + pos); \
            DECODE_ENTRY(payload##sub, tablesub, encodedPayload); \
            firstWord[numHitCandidates] = GETLONG(payload##sub, firstKeyOffset); \
            if (numHitCandidates > hitCandidatesSize + 256) { growHitCandidates(); } \
            ADD_ROW(hitCandidate, row##sub, numHitCandidates, payload##sub); \
	    hits##sub &= hits##sub - 1; \
	} \
	if (empty##sub != 0) break; \
	hash##sub = (hash##sub + 1) & statusMask##tablesub;	\
	LOAD_STATUS(hits##sub, tablesub, hash##sub); \
        empty##sub = hits##sub & 0x8080808080808080L; \
	hits##sub ^= field##sub;          \
	hits##sub -= 0x0101010101010101L; \
	hits##sub &= 0x8080808080808080L ^ empty##sub; \
    }




