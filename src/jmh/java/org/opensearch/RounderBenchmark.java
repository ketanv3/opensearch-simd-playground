package org.opensearch;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@Fork(1)
@BenchmarkMode(Mode.Throughput)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 1, time = 3)
public class RounderBenchmark {

    @Benchmark
    public void round(Blackhole bh, Options opts) {
        Rounder rounder = opts.supplier.get();
        for (int i = 0; i < 1000000; i++) {
            bh.consume(rounder.round(opts.keySupplier.getAsLong()));
        }
    }

    @State(Scope.Benchmark)
    public static class Options {
        @Param({
            "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "12", "14", "16", "18", "20", "22",
            "24", "26", "29", "32", "37", "41", "45", "49", "54", "60", "64", "74", "83", "90",
            "98", "108", "118", "128", "144", "159", "171", "187", "204", "229", "256", "512",
            "1024", "2048", "4096", "8192", "16384", "32768", "65536", "131072"
        })
        public Integer size;

        @Param({
            "binary",
            "linear",
            "hybrid",
            "eytzinger",
            "vectorized_linear",
            "vectorized_eytzinger",
            "unrolled_eytzinger",
            "vectorized_btree"
        })
        public String type;

        public Supplier<Rounder> supplier;
        public LongSupplier keySupplier;

        @Setup
        public void setup() {
            long[] values = new long[size];
            for (int i = 1; i < values.length; i++) {
                values[i] = values[i - 1] + 100;
            }

            long range = values[values.length - 1] - values[0] + 100;
            ThreadLocalRandom tlr = ThreadLocalRandom.current();
            keySupplier = () -> nextPositiveLong(tlr) % range;

            switch (type) {
                case "binary" -> supplier = () -> new Rounder.BinarySearchRounder(values);
                case "linear" -> supplier = () -> new Rounder.LinearSearchRounder(values);
                case "vectorized_linear" -> supplier =
                        () -> new Rounder.VectorizedLinearSearchRounder(values);
                case "eytzinger" -> supplier = () -> new Rounder.EytzingerSearchRounder(values);
                case "vectorized_eytzinger" -> supplier =
                        () -> new Rounder.VectorizedEytzingerSearchRounder(values);
                case "unrolled_eytzinger" -> supplier =
                        () -> new Rounder.UnrolledEytzingerSearchRounder(values);
                case "hybrid" -> supplier = () -> new Rounder.HybridSearchRounder(values);
                case "vectorized_btree" -> supplier = () -> new Rounder.BtreeSearchRounder(values);
                default -> throw new IllegalArgumentException("invalid type: " + type);
            }
        }

        private static long nextPositiveLong(Random random) {
            return random.nextLong() & Long.MAX_VALUE;
        }
    }
}
