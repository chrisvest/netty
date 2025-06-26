/*
 * Copyright 2025 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.util.internal;

import java.lang.reflect.Array;

/**
 * Implementation of multiplicative binary search in level-ordered arrays.
 * <p>
 * This is sometimes known as the Eytzinger layout or Eytzinger search.
 */
public final class LevelOrderBinarySearch {
    private LevelOrderBinarySearch() {
        // Utility class. Static methods only.
    }

    /**
     * Recursive level-ordering, consuming O(log N) stack space.
     */
    private static int intoLevelOrder(Object src, Object dst, int srcIndex, int dstIndex) {
        if (dstIndex < Array.getLength(dst)) {
            srcIndex = intoLevelOrder(src, dst, srcIndex, dstIndex * 2);
            Array.set(dst, dstIndex, Array.get(src, srcIndex));
            srcIndex = intoLevelOrder(src, dst, srcIndex + 1, dstIndex * 2 + 1);
        }
        return srcIndex;
    }

    /**
     * Create a new level-ordered array of shorts from the given sorted array of shorts.
     * The zeroeth element of the returned array is always unused, and the actual values start from index 1.
     *
     * @param array The input array. The values in this array must be in ascending order,
     * as if sorted by {@link java.util.Arrays#sort(short[])}.
     * @return A new array with the values arranged in level-order.
     */
    public static short[] createFromSorted(short... array) {
        short[] eytzinger = new short[array.length + 1];
        intoLevelOrder(array, eytzinger, 0, 1);
        return eytzinger;
    }

    /**
     * Perform a multiplicative binary search for the given needle value, in the given level-ordered array.
     * <p>
     * The array must be created from {@link #createFromSorted(short...)}.
     *
     * @param array The level-ordered array to search in.
     * @param needle The value to search for.
     * @return The array index of the smallest value in the array that is greater than or equal to the needle
     * value, or zero if the needle value is greater than all the values in the array.
     */
    public static int search(short[] array, short needle) {
        int index = 1;
        int length = array.length;
        while (index < length) {
            short value = array[index];
            index <<= 1;
            if (value < needle) {
                index++;
            }
        }
        return index >> Integer.numberOfTrailingZeros(~index) + 1;
    }

    /**
     * Create a new level-ordered array of ints from the given sorted array of ints.
     * The zeroeth element of the returned array is always unused, and the actual values start from index 1.
     *
     * @param array The input array. The values in this array must be in ascending order,
     * as if sorted by {@link java.util.Arrays#sort(int[])}.
     * @return A new array with the values arranged in level-order.
     */
    public static int[] createFromSorted(int... array) {
        int[] eytzinger = new int[array.length + 1];
        intoLevelOrder(array, eytzinger, 0, 1);
        return eytzinger;
    }

    /**
     * Perform a multiplicative binary search for the given needle value, in the given level-ordered array.
     * <p>
     * The array must be created from {@link #createFromSorted(int...)}.
     *
     * @param array The level-ordered array to search in.
     * @param needle The value to search for.
     * @return The array index of the smallest value in the array that is greater than or equal to the needle
     * value, or zero if the needle value is greater than all the values in the array.
     */
    public static int search(int[] array, int needle) {
        int index = 1;
        int length = array.length;
        while (index < length) {
            int value = array[index];
            index <<= 1;
            if (value < needle) {
                index++;
            }
        }
        return index >> Integer.numberOfTrailingZeros(~index) + 1;
    }

    /**
     * Create a new level-ordered array of ints from the given sorted array of ints.
     * The zeroeth element of the returned array is always unused, and the actual values start from index 1.
     *
     * @param array The input array. The values in this array must be in ascending order,
     * as if sorted by {@link java.util.Arrays#sort(int[])}.
     * @return A new array with the values arranged in level-order.
     */
    public static long[] createFromSorted(long... array) {
        long[] eytzinger = new long[array.length + 1];
        intoLevelOrder(array, eytzinger, 0, 1);
        return eytzinger;
    }

    /**
     * Perform a multiplicative binary search for the given needle value, in the given level-ordered array.
     * <p>
     * The array must be created from {@link #createFromSorted(int...)}.
     *
     * @param array The level-ordered array to search in.
     * @param needle The value to search for.
     * @return The array index of the smallest value in the array that is greater than or equal to the needle
     * value, or zero if the needle value is greater than all the values in the array.
     */
    public static int search(long[] array, long needle) {
        int index = 1;
        int length = array.length;
        while (index < length) {
            long value = array[index];
            index <<= 1;
            if (value < needle) {
                index++;
            }
        }
        return index >> Integer.numberOfTrailingZeros(~index) + 1;
    }
}
