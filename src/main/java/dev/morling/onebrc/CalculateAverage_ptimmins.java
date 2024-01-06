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

import java.nio.charset.StandardCharsets;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class CalculateAverage_ptimmins {
    private static final String FILE = "./measurements.txt";
    // private static final String FILE = "./little_measurements.txt";

    private static record Measurement(String station, double value) {
        private Measurement(String[] parts) {
            this(parts[0], Double.parseDouble(parts[1]));
        }
    }

    private static record ResultRow(double min, double mean, double max) {
        public String toString() {
            return round(min) + "/" + round(mean) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    };

    private static class MeasurementAggregator {
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum;
        private long count;

        void merge(MeasurementAggregator other) {
            min = Math.min(min, other.min);
            max = Math.max(max, other.max);
            sum += other.sum;
            count += other.count;
        }
    }

    static class MappedRange {
        MappedByteBuffer mbb;

        boolean frontPad;
        boolean backPad;

        int start;

        // full length including any padding
        int len;

        // probably not useful
        long offset;

        public MappedRange(MappedByteBuffer mbb, boolean frontPad, boolean backPad, int len, long offset) {
            this.mbb = mbb;
            this.frontPad = frontPad;
            this.backPad = backPad;
            this.len = len;
            this.offset = offset;
        }

        @Override
        public String toString() {
            return "MappedRange{" +
                    "mbb=" + mbb +
                    ", frontPad=" + frontPad +
                    ", backPad=" + backPad +
                    ", start=" + start +
                    ", len=" + len +
                    ", offset=" + offset +
                    '}';
        }
    }

    static final long MAX_MMAP_SIZE = Integer.MAX_VALUE;
    static final int padding = 200; // max entry size is 107ish == 100 (station) + 1 (semicolon) + 5 (temp, eg -99.9) + 1 (newline)

    static ArrayList<MappedRange> openMappedFiles(FileChannel channel, int numSplits) throws IOException {
        final long fileSize = channel.size();

        final long avgSize = fileSize / numSplits;
        final long maxSize = (long) (1.5 * avgSize);

        ArrayList<MappedRange> ranges = new ArrayList<>();
        long offset = 0;
        while (offset < fileSize) {
            int size;
            boolean backPad;
            if (offset + maxSize >= fileSize) {
                size = Math.toIntExact(fileSize - offset);
                backPad = false;
            }
            else {
                size = Math.toIntExact(avgSize);
                backPad = true;
            }

            System.out.println("making split with size: " + size);
            boolean frontPad = !ranges.isEmpty();
            MappedByteBuffer mbb = channel.map(FileChannel.MapMode.READ_ONLY, offset, size);
            ranges.add(new MappedRange(mbb, frontPad, backPad, size, offset));

            offset += size;
            if (backPad) {
                offset -= padding;
            }
        }
        return ranges;
    }

    static int findNextEntryStart(MappedByteBuffer mbb, int offset) {
        int curr = offset;
        while (mbb.get(curr) != '\n') {
            curr++;
        }
        curr++;
        return curr;
    }

    static int findNextSemicolon(MappedByteBuffer mbb, int offset) {
        int curr = offset;
        while (mbb.get(curr) != ';') {
            curr++;
        }
        return curr;
    }

    static void processMappedRange(MappedRange range, final HashMap<String, MeasurementAggregator> localAgg) {
        byte[] buf = new byte[200];

        int curr = range.frontPad ? findNextEntryStart(range.mbb, 0) : 0;
        int limit = range.backPad ? findNextEntryStart(range.mbb, range.len - padding) : range.len;

        while (curr < limit) {
            int next = findNextEntryStart(range.mbb, curr);

            int endStr = findNextSemicolon(range.mbb, curr);

            int stationLen = endStr - curr;
            for (int i = 0; i < stationLen; ++i) {
                buf[i] = range.mbb.get(curr);
                curr++;
            }

            String station = new String(buf, 0, stationLen, StandardCharsets.UTF_8); // for UTF-8 encoding

            curr = endStr + 1;

            short sign = 1;
            if (range.mbb.get(curr) == '-') {
                sign = -1;
                curr++;
            }

            int numDigits = next - curr - 2; // subtract one for decimal and one for end newline
            short temp = 0;
            if (numDigits == 3) {
                temp += ((char) range.mbb.get(curr)) - '0';
                temp *= 10;
                temp += ((char) range.mbb.get(curr + 1)) - '0';
                temp *= 10;
                temp += ((char) range.mbb.get(curr + 3)) - '0'; // skip decimal
            }
            else {
                temp += ((char) range.mbb.get(curr)) - '0';
                temp *= 10;
                temp += ((char) range.mbb.get(curr + 2)) - '0'; // skip decimal
            }
            temp *= sign;

            curr = next;

            double tempWithDecimal = ((double) temp) / 10;
            var m = new Measurement(station, tempWithDecimal);
            if (localAgg.containsKey(m.station())) {
                var ma = localAgg.get(m.station());
                ma.count += 1;
                ma.sum += m.value;
                ma.min = Math.min(m.value, ma.min);
                ma.max = Math.max(m.value, ma.max);
            }
            else {
                var ma = new MeasurementAggregator();
                ma.count = 1;
                ma.sum = ma.max = ma.min = m.value;
                localAgg.put(m.station(), ma);
            }
        }
    }

    static HashMap<String, MeasurementAggregator> mergeAggregations(ArrayList<HashMap<String, MeasurementAggregator>> localAggs) {
        HashMap<String, MeasurementAggregator> global = new HashMap<>();

        for (var agg : localAggs) {
            for (Map.Entry<String, MeasurementAggregator> entry : agg.entrySet()) {
                var station = entry.getKey();
                var ma = entry.getValue();

                var currentVal = global.get(station);
                if (currentVal != null) {
                    currentVal.merge(ma);
                }
                else {
                    global.put(station, ma);
                }
            }
        }
        return global;
    }

    public static void main(String[] args) throws IOException, InterruptedException {

        RandomAccessFile file = new RandomAccessFile(FILE, "r");
        FileChannel channel = file.getChannel();

        int numThreads = Runtime.getRuntime().availableProcessors();
        System.out.println("Running on " + numThreads + " threads");

        long fileSize = channel.size();
        int rangesPerThread = 3;
        int numSplits = rangesPerThread * numThreads;

        final ArrayList<MappedRange> mappedRanges = openMappedFiles(channel, numSplits);
        final ArrayList<HashMap<String, MeasurementAggregator>> localAggs = new ArrayList<>(numThreads);

        Thread[] threads = new Thread[numThreads];
        for (int t = 0; t < numThreads; t++) {
            localAggs.add(new HashMap<>());
            int threadId = t;
            threads[t] = new Thread(() -> {
                var localAgg = localAggs.get(threadId);
                for (int split = 0; split < rangesPerThread; ++split) {
                    int splitId = rangesPerThread * threadId + split;
                    processMappedRange(mappedRanges.get(splitId), localAgg);
                }
            }, "Thread-" + t);

            threads[t].start();
        }

        for (var thread : threads) {
            thread.join();
        }

        var globalAggs = mergeAggregations(localAggs);
        Map<String, ResultRow> res = new TreeMap<>();
        for (Map.Entry<String, MeasurementAggregator> entry : globalAggs.entrySet()) {
            var ma = entry.getValue();
            res.put(entry.getKey(), new ResultRow(ma.min, ma.sum / ma.count, ma.max));
        }
        System.out.println(res);
    }
}
