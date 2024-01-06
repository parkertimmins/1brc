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

import static java.util.stream.Collectors.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collector;

public class CalculateAverage_ptimmins {

    private static final String FILE = "./measurements.txt";

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
    }

    // static long endOfLineAfter(MappedByteBuffer mbb, long start, long limit) {
    // long curr = start;
    // while (curr < limit && mbb.getChar(curr) != '\n') {
    //
    // }
    // System.out.print((char) mbb.get());
    // }

    static class MappedRange {
        MappedByteBuffer mbb;

        boolean frontPad;
        boolean backPad;

        int start;

        // full length including any padding
        long len;

        // probably not useful
        long offset;

        public MappedRange(MappedByteBuffer mbb, boolean frontPad, boolean backPad, long len, long offset) {
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

    static ArrayList<MappedRange> openMappedFiles(FileChannel channel) throws IOException {
        final long fileSize = channel.size();
        final long targetSize = 1L << 30; // 1GB
        final long maxSize = 2L << 30; // 2GB
        final long padding = 200; // max entry size is 107ish == 100 (station) + 1 (semicolon) + 5 (temp, eg -99.9) + 1 (newline)

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
                size = targetSize;
                backPad = true;
            }
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

    public static void main(String[] args) throws IOException {

        RandomAccessFile file = new RandomAccessFile(FILE, "r");
        FileChannel channel = file.getChannel();

        ArrayList<MappedRange> mappedRanges = openMappedFiles(channel);

        for (MappedRange range : mappedRanges) {

            System.out.println(range);
        }


        // final HashMap<String, MeasurementAggregator> cache = new HashMap<>();
        // Files.lines(Paths.get(FILE)).forEach(line -> {
        // var m = new Measurement(line.split(";"));
        // if (cache.containsKey(m.station())) {
        // var ma = cache.get(m.station());
        //
        // ma.count += 1;
        // ma.sum += m.value;
        // ma.min = Math.min(m.value, ma.min);
        // ma.max = Math.max(m.value, ma.max);
        // }
        // else {
        // var ma = new MeasurementAggregator();
        // ma.count = 1;
        // ma.sum = m.value;
        // ma.max = m.value;
        // ma.min = m.value;
        // cache.put(m.station(), ma);
        // }
        // });
        //
        // Map<String, ResultRow> res = new TreeMap<>();
        // for (Map.Entry<String, MeasurementAggregator> entry : cache.entrySet()) {
        // var ma = entry.getValue();
        // res.put(entry.getKey(), new ResultRow(ma.min, ma.sum / ma.count, ma.max));
        // }
        // System.out.println(res);
    }
}
