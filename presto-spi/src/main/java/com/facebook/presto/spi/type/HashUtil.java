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
package com.facebook.presto.spi.type;

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.IntArrayBlock;
import com.facebook.presto.spi.block.MapBlock;
import com.facebook.presto.spi.block.MethodHandleUtil;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class HashUtil
{
    private static final long XXHASH64_PRIME64_2 = 0xC2B2AE3D27D4EB4FL;
    private static final long XXHASH64_PRIME64_3 = 0x165667B19E3779F9L;

    static Set<Long> hashes;
    
    private HashUtil() {}


    
    /**
     * <p>
     * This method is copied from {@code finalShuffle} from XxHash64 to provide non-linear transformation to
     * avoid hash collisions when computing checksum over structural types.
     *
     * <p>
     * When both structural hash and checksum are linear functions to input elements,
     * it's vulnerable to hash collision attack by exchanging some input values
     * (e.g. values in the same nested column).
     *
     * <p>
     * Example for checksum collision:
     *
     * <pre>
     * SELECT checksum(value)
     * FROM (VALUES
     *     ARRAY['a', '1'],
     *     ARRAY['b', '2']
     * ) AS t(value)
     * </pre>
     * <p>
     * vs.
     *
     * <pre>
     * SELECT checksum(value)
     * FROM (VALUES
     *     ARRAY['a', '2'],
     *     ARRAY['b', '1']
     * ) AS t(value)
     * </pre>
     *
     * @see io.airlift.slice.XxHash64
     */
    public static long shuffle(long hash)
    {
        hash ^= hash >>> 33;
        hash *= XXHASH64_PRIME64_2;
        hash ^= hash >>> 29;
        hash *= XXHASH64_PRIME64_3;
        hash ^= hash >>> 32;
        return hash;
    }

    static boolean record = true;
    
    public static void checkPage(Page page)
    {
        try {
            if (hashes == null) {
                if (record) {
                    hashes = new HashSet();
                }
                else {
                    hashes = getHashes("/tmp/hashes.txt");
                }
            }
                contains(page, hashes);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    public static boolean contains(Page page, Set<Long> hashes)
    {
        if (page == null) {
            return true;
        }
        MapType mapType = new MapType(
                IntegerType.INTEGER,
                                RealType.REAL,
                //MethodHandleUtil.methodHandle(HashUtil.class, "throwUnsupportedOperation"),
                //MethodHandleUtil.methodHandle(HashUtil.class, "throwUnsupportedOperation"),
                //MethodHandleUtil.methodHandle(HashUtil.class, "throwUnsupportedOperation"),
                //MethodHandleUtil.methodHandle(HashUtil.class, "throwUnsupportedOperation")
                null, null, null, null);

        int channelCount = page.getChannelCount();
        for (int i = 0; i < channelCount; i++) {
            Block block = page.getBlock(i);
            if (block instanceof MapBlock && ((MapBlock) block).getRawValueBlock() instanceof IntArrayBlock) {
                MapBlock map = (MapBlock) block;
                int positionCount = page.getPositionCount();
                for (int j = 0; j < positionCount; j++) {
                    long hash = mapType.hash(map, j);
                    synchronized (hashes) {
                        if (!hashes.contains(hash)) {
                            if (record) {
                                hashes.add(hash);
                            }
                            else {
                                throw new RuntimeException(hash + "is not present");
                            }
                        }
                        }
                }
            }
        }
        return true;
    }

    public static Set<Long> getHashes(String file)
            throws IOException
    {
        Set<Long> hashSet = new HashSet<>();
        List<String> strings = Files.readAllLines(Paths.get(file));
        for (String a : strings) {
            BigInteger bi = new BigInteger(a.replaceAll(" ", ""), 16);
            System.out.println(bi);
            hashSet.add(bi.longValue());
        }
        return hashSet;
    }
}
