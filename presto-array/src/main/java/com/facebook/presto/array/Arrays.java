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

public class Arrays
{
    private Arrays() {}

    public static int[] ensureCapacity(int[] buffer, int capacity)
    {
        return ensureCapacity(buffer, capacity, 1.0f, false, false);
    }

    public static int[] ensureCapacity(int[] buffer, int capacity, float expansionFactor, boolean needsCopy, boolean needsInitialization)
    {
        int newCapacity = (int) (capacity * expansionFactor);

        if (buffer == null) {
            buffer = new int[newCapacity];
        }

        if (buffer.length < capacity) {
            if (needsCopy) {
                buffer = java.util.Arrays.copyOf(buffer, newCapacity);
            }
            else {
                buffer = new int[newCapacity];
            }
        }
        else if (needsInitialization) {
            java.util.Arrays.fill(buffer, 0);
        }

        return buffer;
    }

    public static long[] ensureCapacity(long[] buffer, int capacity)
    {
        if (buffer == null || buffer.length < capacity) {
            return new long[capacity];
        }

        return buffer;
    }

    public static boolean[] ensureCapacity(boolean[] buffer, int capacity)
    {
        if (buffer == null || buffer.length < capacity) {
            return new boolean[capacity];
        }

        return buffer;
    }

    public static byte[] ensureCapacity(byte[] buffer, int capacity)
    {
        if (buffer == null || buffer.length < capacity) {
            return new byte[capacity];
        }

        return buffer;
    }

    public static byte[] ensureCapacity(byte[] buffer, int capacity, float expansionFactor, boolean needsCopy)
    {
        checkValidExpansionFactor(expansionFactor);

        int newCapacity = (int) (capacity * expansionFactor);

        if (buffer == null) {
            buffer = new byte[newCapacity];
        }

        if (buffer.length < capacity) {
            if (needsCopy) {
                buffer = java.util.Arrays.copyOf(buffer, newCapacity);
            }
            else {
                buffer = new byte[newCapacity];
            }
        }

        return buffer;
    }

    private static void checkValidExpansionFactor(float expansionFactor)
    {
        if (expansionFactor < 1) {
            throw new IllegalArgumentException("expansionFactor should be greater than 1");
        }
    }
}
