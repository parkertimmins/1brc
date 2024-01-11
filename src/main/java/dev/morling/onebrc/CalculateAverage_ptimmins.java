/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorSpecies;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class CalculateAverage_ptimmins {
    private static final String FILE = "./measurements.txt";
    // private static final String FILE = "./full_measurements.no_license";

    private static record ResultRow(double min, double mean, double max) {
        public String toString() {
            return round(min) + "/" + round(mean) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    };

    private static class MeasurementAggregator {
        private short min;
        private short max;
        private long sum;
        private long count;

        void merge(MeasurementAggregator other) {
            min = (short) Math.min(min, other.min);
            max = (short) Math.max(max, other.max);
            sum += other.sum;
            count += other.count;
        }

        void merge(OpenHashTable.Entry other) {
            min = (short) Math.min(min, other.min);
            max = (short) Math.max(max, other.max);
            sum += other.sum;
            count += other.count;
        }

        static MeasurementAggregator fromEntry(OpenHashTable.Entry other) {
            var ma = new MeasurementAggregator();
            ma.min = other.min;
            ma.max = other.max;
            ma.sum = other.sum;
            ma.count = other.count;
            return ma;
        }
    }

    static class OpenHashTable {
        static class Entry {
            byte[] key;
            short min;
            short max;
            long sum = 0;
            long count = 0;
            int hash;
        }

        static final int bits = 14;
        static final int tableSize = 1 << bits; // 16k
        static final int mask = tableSize - 1;

        final Entry[] entries = new Entry[tableSize];

        void add(byte[] buf, int sLen, short val, int hash) {
            int idx = hash & mask;

            while (true) {
                Entry entry = entries[idx];

                // key not present, so add it
                if (entry == null) {
                    entry = entries[idx] = new Entry();
                    entry.key = Arrays.copyOf(buf, sLen);
                    entry.min = entry.max = val;
                    entry.sum += val;
                    entry.count++;
                    entry.hash = hash;
                    break;
                }
                else if (entry.key.length == sLen && Arrays.equals(entry.key, 0, sLen, buf, 0, sLen)) {
                    entry.min = (short) Math.min(entry.min, val);
                    entry.max = (short) Math.max(entry.max, val);
                    entry.sum += val;
                    entry.count++;
                    break;
                }
                else {
                    idx = (idx + 1) & mask;
                }
            }
        }
    }

    static boolean eq(byte[] a, byte[] b, int len) {
        return Arrays.equals(a, 0, len, b, 0, len);
    }

    static class MappedRange {
        MemorySegment ms;

        boolean frontPad;
        boolean backPad;

        long start;

        // full length including any padding
        long len;

        // probably not useful
        long offset;

        public MappedRange(MemorySegment ms, boolean frontPad, boolean backPad, long len, long offset) {
            this.ms = ms;
            this.frontPad = frontPad;
            this.backPad = backPad;
            this.len = len;
            this.offset = offset;
        }

        @Override
        public String toString() {
            return "MappedRange{" +
                    "ms=" + ms +
                    ", frontPad=" + frontPad +
                    ", backPad=" + backPad +
                    ", start=" + start +
                    ", len=" + len +
                    ", offset=" + offset +
                    '}';
        }
    }

    static final int padding = 200; // max entry size is 107ish == 100 (station) + 1 (semicolon) + 5 (temp, eg -99.9) + 1 (newline)

    // static long findNextEntryStart(MemorySegment ms, long offset) {
    // long curr = offset;
    // while (ms.get(ValueLayout.JAVA_BYTE, curr) != '\n') {
    // curr++;
    // }
    // curr++;
    // return curr;
    // }

    // TODO dont call if section is too short!
    static final ByteVector newlinePattern = ByteVector.broadcast(ByteVector.SPECIES_256, (byte) '\n');

    static long findNextEntryStart(MemorySegment ms, long offset) {
        long curr = offset;

        var section = ByteVector.fromMemorySegment(ByteVector.SPECIES_256, ms, curr, ByteOrder.LITTLE_ENDIAN);
        var idx = section.eq(newlinePattern).firstTrue();
        if (idx != 32)
            return curr + idx + 1;
        curr += 32;
        section = ByteVector.fromMemorySegment(ByteVector.SPECIES_256, ms, curr, ByteOrder.LITTLE_ENDIAN);
        idx = section.eq(newlinePattern).firstTrue();
        if (idx != 32)
            return curr + idx + 1;
        curr += 32;
        section = ByteVector.fromMemorySegment(ByteVector.SPECIES_256, ms, curr, ByteOrder.LITTLE_ENDIAN);
        idx = section.eq(newlinePattern).firstTrue();
        if (idx != 32)
            return curr + idx + 1;
        curr += 32;
        section = ByteVector.fromMemorySegment(ByteVector.SPECIES_256, ms, curr, ByteOrder.LITTLE_ENDIAN);
        idx = section.eq(newlinePattern).firstTrue();
        if (idx != 32)
            return curr + idx + 1;

        // unreachable
        return -1;
    }

    static short[] digits10s = { 0, 100, 200, 300, 400, 500, 600, 700, 800, 900 };
    static short[] digits1s = { 0, 10, 20, 30, 40, 50, 60, 70, 80, 90 };

    static void processScalarEnd(MemorySegment ms, long start, long end, final OpenHashTable localAgg) {
        byte[] buf = new byte[128];

        long curr = start;
        long limit = end;

        while (curr < limit) {

            int i = 0;
            byte val = ms.get(ValueLayout.JAVA_BYTE, curr);
            while (val != ';') {
                buf[i] = val;
                i++;
                curr++;
                val = ms.get(ValueLayout.JAVA_BYTE, curr);
            }

            int sLen = i;
            int hash = hash(buf, sLen);

            curr++; // skip semicolon

            short sign = 1;
            if (ms.get(ValueLayout.JAVA_BYTE, curr) == '-') {
                sign = -1;
                curr++;
            }

            int numDigits = ms.get(ValueLayout.JAVA_BYTE, curr + 2) == '.' ? 3 : 2;
            short temp = 0;
            if (numDigits == 3) {
                temp += digits10s[((char) ms.get(ValueLayout.JAVA_BYTE, curr)) - '0'];
                temp += digits1s[((char) ms.get(ValueLayout.JAVA_BYTE, curr + 1)) - '0'];
                temp += ((char) ms.get(ValueLayout.JAVA_BYTE, curr + 3)) - '0'; // skip decimal
                curr += 5;
            }
            else {
                temp += digits1s[((char) ms.get(ValueLayout.JAVA_BYTE, curr)) - '0'];
                temp += ((char) ms.get(ValueLayout.JAVA_BYTE, curr + 2)) - '0'; // skip decimal
                curr += 4;
            }
            temp *= sign;

            localAgg.add(buf, sLen, temp, hash);
        }
    }

    static int hash(byte[] buf, int sLen) {
        // if shorter than 8 chars, mask out upper bits
        long mask = sLen < 8 ? -(1L << ((8 - sLen) << 3)) : 0xFFFFFFFFL;
        long val = ((buf[0] & 0xffL) << 56) | ((buf[1] & 0xffL) << 48) | ((buf[2] & 0xffL) << 40) | ((buf[3] & 0xffL) << 32) | ((buf[4] & 0xffL) << 24)
                | ((buf[5] & 0xffL) << 16) | ((buf[6] & 0xFFL) << 8) | (buf[7] & 0xffL);
        // long val1 = ByteBuffer.wrap(buf, 0, 8).getLong();
        val &= mask;

        // lemire: https://lemire.me/blog/2023/07/14/recognizing-string-prefixes-with-simd-instructions/
        int hash = (int) (((((val >> 32) ^ val) & 0xffffffffL) * 3523216699L) >> 32);
        return hash;
        // https://lemire.me/blog/2015/10/22/faster-hashing-without-effort/
    }

    static void processMappedRange(MemorySegment ms, boolean frontPad, boolean backPad, long start, long end, final OpenHashTable localAgg) {
        byte[] buf = new byte[128]; // TODO move into local start so doesn't GC
        long curr = frontPad ? findNextEntryStart(ms, start) : start;
        long limit = end - padding;
        long entryStart = curr;

        // ";9.9\na;9.9\na;9.9\na;9.9\na;9.9\na;9" - 32 char section with most possible semicolons, that being 6
        int[] indices = new int[6]; // can make short maybe
        while (curr < limit) {
            var section = ByteVector.fromMemorySegment(ByteVector.SPECIES_256, ms, curr, ByteOrder.LITTLE_ENDIAN);
            var semiMatchesMask = section.eq((byte) ';');
            var semiMatches = semiMatchesMask.toLong();

            // https://arxiv.org/pdf/1902.08318.pdf
            int ptr = 0;
            int numBits = Long.bitCount(semiMatches);
            indices[ptr++] = Long.numberOfTrailingZeros(semiMatches);
            semiMatches &= (semiMatches - 1);
            indices[ptr++] = Long.numberOfTrailingZeros(semiMatches);
            semiMatches &= (semiMatches - 1);
            indices[ptr++] = Long.numberOfTrailingZeros(semiMatches);
            semiMatches &= (semiMatches - 1);
            indices[ptr++] = Long.numberOfTrailingZeros(semiMatches);
            semiMatches &= (semiMatches - 1);
            indices[ptr++] = Long.numberOfTrailingZeros(semiMatches);
            semiMatches &= (semiMatches - 1);
            indices[ptr] = Long.numberOfTrailingZeros(semiMatches);
            // semiMatches &= (semiMatches - 1);

            for (int bitIdx = 0; bitIdx < numBits; ++bitIdx) {
                int idx = indices[bitIdx];
                final long semiIdx = curr + idx;

                int j = 0;
                for (long i = entryStart; i < semiIdx; ++i, ++j) {
                    buf[j] = ms.get(ValueLayout.JAVA_BYTE, i);
                }
                int sLen = j;
                int hash = hash(buf, sLen);

                // Parse temperature
                long tempIdx = semiIdx + 1;
                boolean neg = ms.get(ValueLayout.JAVA_BYTE, tempIdx) == '-';
                boolean twoDig = ms.get(ValueLayout.JAVA_BYTE, tempIdx + 1 + (neg ? 1 : 0)) == '.';
                int len = 3 + (neg ? 1 : 0) + (twoDig ? 0 : 1);
                int d0 = ((char) ms.get(ValueLayout.JAVA_BYTE, tempIdx + len - 1)) - '0';
                int d1 = ((char) ms.get(ValueLayout.JAVA_BYTE, tempIdx + len - 3)) - '0';
                int base = d0 + digits1s[d1] + (twoDig ? 0 : digits10s[((char) ms.get(ValueLayout.JAVA_BYTE, tempIdx + len - 4)) - '0']);
//                int base = d0 + d1 * 10 + (twoDig ? 0 : (((char) ms.get(ValueLayout.JAVA_BYTE, tempIdx + len - 4)) - '0') * 100);
                short temp = (short) (neg ? -base : base);

                localAgg.add(buf, sLen, temp, hash);
                entryStart = tempIdx + len + 1;
            }

            curr += 32;
        }

        // finish with scalars
        if (!backPad) {
            processScalarEnd(ms, entryStart, end, localAgg);
        }
    }

    private static final VectorSpecies<Byte> SPECIES = ByteVector.SPECIES_256;

    static HashMap<String, MeasurementAggregator> mergeAggregations(ArrayList<OpenHashTable> localAggs) {
        HashMap<String, MeasurementAggregator> global = new HashMap<>();

        // HashSet<Integer> uniquesHashValues = new HashSet<Integer>();
        // HashSet<String> uniqueCities = new HashSet<String>();
        // HashMap<String, HashSet<Integer>> cityToHash = new HashMap<>();

        for (var agg : localAggs) {
            for (OpenHashTable.Entry entry : agg.entries) {

                if (entry == null) {
                    continue;
                }
                // uniquesHashValues.add(entry.hash);

                String station = new String(entry.key, StandardCharsets.UTF_8); // for UTF-8 encoding
                // uniqueCities.add(station);
                //
                // if (!cityToHash.containsKey(station)) {
                // cityToHash.put(station, new HashSet<>());
                // }
                // cityToHash.get(station).add(entry.hash);

                var currentVal = global.get(station);
                if (currentVal != null) {
                    currentVal.merge(entry);
                }
                else {
                    global.put(station, MeasurementAggregator.fromEntry(entry));
                }
            }
        }

        //
        // for (var pair : cityToHash.entrySet()) {
        // if (pair.getValue().size() > 1) {
        // System.err.println("multiple hashes: " + pair.getKey() + " " + pair.getValue());
        // }
        // }

        // System.err.println("Unique stations: " + uniqueCities.size() + ", unique hash values: " + uniquesHashValues.size());
        return global;
    }

    public static void main(String[] args) throws IOException, InterruptedException {

        RandomAccessFile file = new RandomAccessFile(FILE, "r");
        FileChannel channel = file.getChannel();

        // int numThreads = 1;
        int numThreads = Runtime.getRuntime().availableProcessors();
        System.out.println("Running on " + numThreads + " threads");

        final long fileSize = channel.size();
        final MemorySegment ms = channel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize, Arena.global());
        final ArrayList<OpenHashTable> localAggs = new ArrayList<>(numThreads);

        // final long batchSize = fileSize / numThreads + 1;
        final long batchSize = 10_000_000;

        long startTime = System.currentTimeMillis();
        Thread[] threads = new Thread[numThreads];

        final AtomicLong progress = new AtomicLong(0);

        class Task implements Runnable {
            final int threadId;

            Task(int threadId) {
                this.threadId = threadId;
            }

            @Override
            public void run() {
                var localAgg = localAggs.get(threadId);
                while (progress.get() < fileSize) {
                    long startBatch = progress.getAndAdd(batchSize);
                    long endBatch = Math.min(startBatch + batchSize, fileSize);
                    boolean first = startBatch == 0;
                    boolean frontPad = !first;
                    boolean last = endBatch == fileSize;
                    boolean backPad = !last;
                    // System.err.println("Thread " + threadId + " processing " + startBatch + " to " + endBatch + " at " + (System.currentTimeMillis() - startTime));
                    processMappedRange(ms, frontPad, backPad, startBatch, endBatch, localAgg);
                }
            }
        }

        for (int t = 0; t < numThreads; t++) {
            localAggs.add(new OpenHashTable());
            threads[t] = new Thread(new Task(t), "Thread-" + t);
            threads[t].start();
        }

        for (var thread : threads) {
            thread.join();
        }

        System.err.println("Start merge at " + (System.currentTimeMillis() - startTime));

        var globalAggs = mergeAggregations(localAggs);

        System.err.println("End merge at " + (System.currentTimeMillis() - startTime));
        Map<String, ResultRow> res = new TreeMap<>();
        for (Map.Entry<String, MeasurementAggregator> entry : globalAggs.entrySet()) {
            var ma = entry.getValue();
            res.put(entry.getKey(), new ResultRow(ma.min / 10.0, (ma.sum / 10.0) / ma.count, ma.max / 10.0));
        }
        System.out.println(res);
    }
}
