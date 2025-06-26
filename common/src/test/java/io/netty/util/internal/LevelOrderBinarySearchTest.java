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

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

import static io.netty.util.internal.LevelOrderBinarySearch.createFromSorted;
import static io.netty.util.internal.LevelOrderBinarySearch.search;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class LevelOrderBinarySearchTest {
    @SuppressWarnings({"ZeroLengthArrayAllocation", "RedundantArrayCreation"})
    @Test
    void emptyArrays() throws Exception {
        assertEquals(0, search(createFromSorted(new short[0]), (short) 0));
        assertEquals(0, search(createFromSorted(new int[0]), 0));
        assertEquals(0, search(createFromSorted(new long[0]), 0L));

        assertEquals(0, search(createFromSorted(new short[0]), (short) 1));
        assertEquals(0, search(createFromSorted(new int[0]), 1));
        assertEquals(0, search(createFromSorted(new long[0]), 1L));

        assertEquals(0, search(createFromSorted(new short[0]), (short) -1));
        assertEquals(0, search(createFromSorted(new int[0]), -1));
        assertEquals(0, search(createFromSorted(new long[0]), -1L));
    }

    @RepeatedTest(100)
    void testShorts() {
        int[] sortedInts = ThreadLocalRandom.current().ints(100, 0, 1000).sorted().distinct().toArray();
        short[] sorted = new short[sortedInts.length];
        for (int i = 0; i < sortedInts.length; i++) {
            sorted[i] = (short) sortedInts[i];
        }
        short[] eytzinger = createFromSorted(sorted);
        short maxValue = sorted[sorted.length - 1];
        for (short i = 0; i < 1000; i++) {
            int expectedLookup = Arrays.binarySearch(sorted, i);
            int actualLookup = search(eytzinger, i);
            if (expectedLookup >= 0) {
                assertEquals(sorted[expectedLookup], eytzinger[actualLookup], "for value " + i);
            } else if (i > maxValue) {
                assertEquals(0, actualLookup);
            } else {
                assertThat(eytzinger[actualLookup]).isGreaterThan(i);
            }
        }
    }

    @RepeatedTest(100)
    void testInts() {
        int[] sorted = ThreadLocalRandom.current().ints(100, 0, 1000).sorted().distinct().toArray();
        int[] eytzinger = createFromSorted(sorted);
        int maxValue = sorted[sorted.length - 1];
        for (int i = 0; i < 1000; i++) {
            int expectedLookup = Arrays.binarySearch(sorted, i);
            int actualLookup = search(eytzinger, i);
            if (expectedLookup >= 0) {
                assertEquals(sorted[expectedLookup], eytzinger[actualLookup], "for value " + i);
            } else if (i > maxValue) {
                assertEquals(0, actualLookup);
            } else {
                assertThat(eytzinger[actualLookup]).isGreaterThan(i);
            }
        }
    }

    @RepeatedTest(100)
    void testLongs() {
        long[] sorted = ThreadLocalRandom.current().ints(100, 0, 1000).sorted().distinct().asLongStream().toArray();
        long[] eytzinger = createFromSorted(sorted);
        long maxValue = sorted[sorted.length - 1];
        for (long i = 0; i < 1000; i++) {
            int expectedLookup = Arrays.binarySearch(sorted, i);
            int actualLookup = search(eytzinger, i);
            if (expectedLookup >= 0) {
                assertEquals(sorted[expectedLookup], eytzinger[actualLookup], "for value " + i);
            } else if (i > maxValue) {
                assertEquals(0, actualLookup);
            } else {
                assertThat(eytzinger[actualLookup]).isGreaterThan(i);
            }
        }
    }
}
