package org.opensearch;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
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
        Rounder rounder = opts.type.get(opts.values);
        for (long key : opts.keys) {
            bh.consume(rounder.round(key));
        }
    }

    @State(Scope.Benchmark)
    public static class Options {
        private static final int NUM_KEYS = 1000000;

        @Param({
            "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "12", "14", "16", "18", "20", "22",
            "24", "26", "29", "32", "37", "41", "45", "49", "54", "60", "64", "74", "83", "90",
            "98", "108", "118", "128", "144", "159", "171", "187", "204", "229", "256"
        })
        public Integer size;

        @Param({
            "BINARY",
            "LINEAR",
            "HYBRID",
            "EYTZINGER",
            "VECTORIZED_LINEAR",
            "VECTORIZED_EYTZINGER",
            "UNROLLED_EYTZINGER",
            "VECTORIZED_BTREE"
        })
        public Type type;

        @Param({"RANDOM", "SORTED"})
        public Distribution distribution;

        public long[] values;
        public long[] keys;

        @Setup
        public void setup() {
            values = new long[size];
            for (int i = 1; i < values.length; i++) {
                values[i] = values[i - 1] + 100;
            }

            keys = new long[NUM_KEYS];
            long range = values[values.length - 1] - values[0] + 100;
            ThreadLocalRandom tlr = ThreadLocalRandom.current();
            for (int i = 0; i < keys.length; i++) {
                keys[i] = nextPositiveLong(tlr) % range;
            }
            if (distribution == Distribution.SORTED) {
                Arrays.sort(keys);
            }
        }
    }

    private static long nextPositiveLong(Random random) {
        return random.nextLong() & Long.MAX_VALUE;
    }

    public enum Distribution {
        RANDOM,
        SORTED
    }

    public enum Type {
        BINARY(Rounder.BinarySearchRounder::new),
        LINEAR(Rounder.LinearSearchRounder::new),
        VECTORIZED_LINEAR(Rounder.VectorizedLinearSearchRounder::new),
        EYTZINGER(Rounder.EytzingerSearchRounder::new),
        VECTORIZED_EYTZINGER(Rounder.VectorizedEytzingerSearchRounder::new),
        UNROLLED_EYTZINGER(Rounder.UnrolledEytzingerSearchRounder::new),
        HYBRID(Rounder.HybridSearchRounder::new),
        VECTORIZED_BTREE(Rounder.BtreeSearchRounder::new);

        private final Function<long[], Rounder> rounder;

        Type(Function<long[], Rounder> rounder) {
            this.rounder = rounder;
        }

        public Rounder get(long[] values) {
            return rounder.apply(values);
        }
    }
}
