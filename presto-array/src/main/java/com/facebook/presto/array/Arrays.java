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
package com.facebook.presto.array;

import static com.facebook.presto.array.Arrays.ExpansionFactor.SMALL;
import static com.facebook.presto.array.Arrays.ExpansionOption.INITIALIZE;
import static com.facebook.presto.array.Arrays.ExpansionOption.NONE;
import static com.facebook.presto.array.Arrays.ExpansionOption.PRESERVE;

public class Arrays
{
    private Arrays() {}

    public static int[] ensureCapacity(int[] buffer, int capacity)
    {
        return ensureCapacity(buffer, capacity, SMALL, NONE);
    }

    public static int[] ensureCapacity(int[] buffer, int capacity, ExpansionFactor expansionFactor, ExpansionOption expansionOption)
    {
        int newCapacity = (int) (capacity * expansionFactor.expansionFactor);

        if (buffer == null) {
            buffer = new int[newCapacity];
        }
        else if (buffer.length < capacity) {
            if (expansionOption == PRESERVE) {
                int oldSize = buffer.length;
                buffer = java.util.Arrays.copyOf(buffer, newCapacity);
                debugFill(buffer, 0);
            }
            else {
                buffer = new int[newCapacity];
                debugFill(buffer, 0);
            }
        }
        else if (expansionOption == INITIALIZE) {
            java.util.Arrays.fill(buffer, 0);
        }
        else if (expansionOption != PRESERVE) {
            debugFill(buffer, 0);
        }

        return buffer;
    }

    public static long[] ensureCapacity(long[] buffer, int capacity)
    {
        if (buffer == null || buffer.length < capacity) {
            buffer = new long[(int) (capacity * SMALL.expansionFactor)];
        }
        debugFill(buffer, 0);
        return buffer;
    }

    public static boolean[] ensureCapacity(boolean[] buffer, int capacity)
    {
        if (buffer == null || buffer.length < capacity) {
            buffer = new boolean[(int) (capacity * SMALL.expansionFactor)];
        }

        debugFill(buffer, 0);
        return buffer;
    }

    public static byte[] ensureCapacity(byte[] buffer, int capacity)
    {
        if (buffer == null || buffer.length < capacity) {
            buffer = new byte[(int) (capacity * SMALL.expansionFactor)];
        }

        debugFill(buffer, 0);
        return buffer;
    }

    public static int[][] ensureCapacity(int[][] buffer, int capacity)
    {
        if (buffer == null || buffer.length < capacity) {
            buffer = new int[capacity][];
        }

        debugFill(buffer, 0);
        return buffer;
    }

    public static boolean[][] ensureCapacity(boolean[][] buffer, int capacity)
    {
        if (buffer == null || buffer.length < capacity) {
            buffer = new boolean[capacity][];
        }
        debugFill(buffer, 0);

        return buffer;
    }

    public static byte[] ensureCapacity(byte[] buffer, int capacity, ExpansionFactor expansionFactor, ExpansionOption expansionOption)
    {
        int newCapacity = (int) (capacity * expansionFactor.expansionFactor);

        if (buffer == null) {
            buffer = new byte[newCapacity];
        }
        else if (buffer.length < capacity) {
            if (expansionOption == PRESERVE) {
                int oldCapacity = buffer.length;
                buffer = java.util.Arrays.copyOf(buffer, newCapacity);
                debugFill(buffer, oldCapacity);
            }
            else {
                buffer = new byte[newCapacity];
            }
        }
        else if (expansionOption == INITIALIZE) {
            java.util.Arrays.fill(buffer, (byte) 0);
        }
        else if (expansionOption != PRESERVE) {
            debugFill(buffer, 0);
        }

        return buffer;
    }

    private static void debugFill(byte[] array, int begin)
    {
        for (int i = begin; i < array.length; i++) {
            array[i] = (byte) i;
        }
    }

    private static void debugFill(boolean[] array, int begin)
    {
        for (int i = begin; i < array.length; i++) {
            array[i] = (i & 1) == 0;
        }
    }
    
    private static void debugFill(int[] array, int begin)
    {
        for (int i = begin; i < array.length; i++) {
            array[i] = -1000000000 - i;
        }
    }

    private static void debugFill(int[][] array, int begin)
    {
        for (int i = begin; i < array.length; i++) {
            array[i] = null;
        }
    }

    private static void debugFill(int[][][] array, int begin)
    {
        for (int i = begin; i < array.length; i++) {
            array[i] = null;
        }
    }

    private static void debugFill(boolean[][] array, int begin)
    {
        for (int i = begin; i < array.length; i++) {
            array[i] = null;
        }
    }

    
    
    private static void debugFill(long[] array, int begin)
    {
        for (int i = begin; i < array.length; i++) {
            array[i] = -10000000000000L - i;
        }
    }
        
    
    public enum ExpansionFactor
    {
        SMALL(1.0),
        MEDIUM(1.5),
        LARGE(2.0);

        private final double expansionFactor;

        ExpansionFactor(double expansionFactor)
        {
            this.expansionFactor = expansionFactor;
        }
    }

    public enum ExpansionOption
    {
        PRESERVE,
        INITIALIZE,
        NONE;
    }
}
