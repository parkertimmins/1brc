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
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

import java.lang.foreign.ValueLayout;
import java.lang.reflect.Array;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32C;

public class CalculateAverage_ptimmins {
    private static final String FILE = "./measurements.txt";
    // private static final String FILE = "./little_measurements.txt";

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
    //
    //
    // static class OpenHashTable {
    // static final int numEntries = 20000;
    //
    // // entry fields
    // // 2 - non-inline string index
    // // 10ish - str chars (if small enough)
    // // string hash ?
    // // 2 min
    // // 2 max
    // // 8 sum
    // // 8 count
    // //
    // static private final int constantSize = 2 + 2 + 2 + 8 + 8;
    //
    //
    // private final byte[] bytes;
    // private final int numInlineStrSize;
    // private final int elementSize;
    //
    // private final ArrayList<String> nonInlineStrs = new ArrayList<>();
    //
    // OpenHashTable(int numInlineStrSize) {
    // this.numInlineStrSize = numInlineStrSize;
    // this.elementSize = numInlineStrSize + constantSize;
    // this.bytes = new byte[elementSize * numEntries];
    // }
    //
    // short getNonInlineIdx(int index) {
    // final int elementStart = index * elementSize;
    // return (short) (bytes[elementStart] << 8 + bytes[elementStart]);
    // }
    //
    //
    // };
    //

    static class OpenHashTable {
        static class Entry {
            byte[] key;
            short min;
            short max;
            long sum;
            long count;
        }

        static final int bits = 14;
        static final int tableSize = 1 << bits; // 16k
        static final int shift = 32 - bits - 1;
        static final int mask = tableSize - 1;

        final Entry[] entries = new Entry[tableSize];

        void add(byte[] buf, int sLen, short val, int hash) {
            int idx = (hash >> shift) & mask;

            while (true) {
                Entry entry = entries[idx];

                // key not present, so add it
                if (entry == null) {
                    entry = entries[idx] = new Entry();
                    entry.key = Arrays.copyOf(buf, sLen);
                    // entry.key = new String(buf, 0, sLen, StandardCharsets.UTF_8); // for UTF-8 encoding
                    entry.min = entry.max = val;
                    entry.sum = val;
                    entry.count = 1;
                    break;
                }
                else {
                    if (entry.key.length == sLen && Arrays.equals(entry.key, 0, sLen, buf, 0, sLen)) {
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

    static final long batchSize = 10_000_000;
    static final int padding = 200; // max entry size is 107ish == 100 (station) + 1 (semicolon) + 5 (temp, eg -99.9) + 1 (newline)

    static ArrayList<MappedRange> openMappedFiles(FileChannel channel, int numSplits) throws IOException {

        final long fileSize = channel.size();
        MemorySegment ms = channel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize, Arena.global());

        final long avgSize = fileSize / numSplits;
        final long maxSize = (long) (1.5 * avgSize);

        ArrayList<MappedRange> ranges = new ArrayList<>();
        long offset = 0;
        while (offset < fileSize) {
            long size;
            boolean backPad;
            if (offset + maxSize >= fileSize) {
                size = fileSize - offset;
                backPad = false;
            }
            else {
                size = avgSize;
                backPad = true;
            }

            System.out.println("making split with size: " + size);
            boolean frontPad = !ranges.isEmpty();
            ranges.add(new MappedRange(ms, frontPad, backPad, size, offset));

            offset += size;
            if (backPad) {
                offset -= padding;
            }
        }
        return ranges;
    }

    static long findNextEntryStart(MemorySegment ms, long offset) {
        long curr = offset;
        while (ms.get(ValueLayout.JAVA_BYTE, curr) != '\n') {
            curr++;
        }
        curr++;
        return curr;
    }

    static short[] digits10s = { 0, 100, 200, 300, 400, 500, 600, 700, 800, 900 };
    static short[] digits1s = { 0, 10, 20, 30, 40, 50, 60, 70, 80, 90 };

    static void processScalarEnd(MemorySegment ms, long start, long end, final OpenHashTable localAgg) {
        byte[] buf = new byte[128];

        long curr = start;
        long limit = end;

        CRC32C crc32c = new CRC32C();
        while (curr < limit) {

            int i = 0;
            byte val = ms.get(ValueLayout.JAVA_BYTE, curr);
            crc32c.reset();
            while (val != ';') {
                crc32c.update(val);
                buf[i] = val;
                i++;
                curr++;
                val = ms.get(ValueLayout.JAVA_BYTE, curr);
            }

            int sLen = i;

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

            localAgg.add(buf, sLen, temp, (int) crc32c.getValue());
        }
    }


    static void processMappedRange(MemorySegment ms, boolean frontPad, boolean backPad, long start, long end, final OpenHashTable localAgg) {
        byte[] buf = new byte[128];

        long curr = frontPad ? findNextEntryStart(ms, start) : start;
        long limit = end - padding;

        var needle = ByteVector.broadcast(ByteVector.SPECIES_256, ';');
        CRC32C crc32c = new CRC32C();
        while (curr < limit) {

            int segStart = 0;
            int sLen;

            while (true) {
                var section = ByteVector.fromMemorySegment(ByteVector.SPECIES_256, ms, curr + segStart, ByteOrder.LITTLE_ENDIAN);
                section.intoArray(buf, segStart);
                VectorMask<Byte> matches = section.compare(VectorOperators.EQ, needle);
                int idx = matches.firstTrue();
                if (idx != 32) {
                    sLen = segStart + idx;
                    break;
                }
                segStart += 32;
            }

            crc32c.reset();
            for (int i = 0; i < sLen; i++) {
                crc32c.update(buf[i]);
            }

            curr += sLen;
            curr++; //semicolon

            short sign = 1;
            if (ms.get(ValueLayout.JAVA_BYTE, curr) == '-') {
                sign = -1;
                curr++;
            }

            int numDigits = ms.get(ValueLayout.JAVA_BYTE, curr + 2) == '.' ? 3 : 2;
            short temp;
            if (numDigits == 3) {
                int d10s = ((char) ms.get(ValueLayout.JAVA_BYTE, curr)) - '0';
                int d1s = ((char) ms.get(ValueLayout.JAVA_BYTE, curr+1)) - '0';
                int d0s = ((char) ms.get(ValueLayout.JAVA_BYTE, curr+3)) - '0';
                temp = (short) (digits10s[d10s] + digits1s[d1s] + d0s);
                curr += 5;
            }
            else {
                int d1s = ((char) ms.get(ValueLayout.JAVA_BYTE, curr)) - '0';
                int d0s = ((char) ms.get(ValueLayout.JAVA_BYTE, curr + 2)) - '0';
                temp = (short) (digits1s[d1s] + d0s);
                curr += 4;
            }
            temp *= sign;

            localAgg.add(buf, sLen, temp, (int) crc32c.getValue());
        }

        // finish with scalars
        if (!backPad) {
            processScalarEnd(ms, curr, end, localAgg);
        }
    }

    static HashMap<String, MeasurementAggregator> mergeAggregations(ArrayList<OpenHashTable> localAggs) {
        HashMap<String, MeasurementAggregator> global = new HashMap<>();

        for (var agg : localAggs) {
            for (OpenHashTable.Entry entry : agg.entries) {

                if (entry == null) {
                    continue;
                }

                String station = new String(entry.key, StandardCharsets.UTF_8); // for UTF-8 encoding
                var currentVal = global.get(station);
                if (currentVal != null) {
                    currentVal.merge(entry);
                }
                else {
                    global.put(station, MeasurementAggregator.fromEntry(entry));
                }
            }
        }
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
                    System.err.println("Thread " + threadId + " processing " + startBatch + " to " + endBatch + " at " + (System.currentTimeMillis() - startTime));
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
