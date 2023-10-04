package org.opensearch;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Random;
import org.junit.jupiter.api.RepeatedTest;

public class RounderTest {

    @RepeatedTest(value = 10)
    public void testImplementations() {
        Random random = new Random();
        long[] values = new long[10 + Math.abs(random.nextInt()) % 2500];
        for (int i = 1; i < values.length; i++) {
            values[i] = values[i - 1] + Math.abs(random.nextLong()) % 100;
        }

        Rounder[] impls = {
            new Rounder.BinarySearchRounder(values),
            new Rounder.LinearSearchRounder(values),
            new Rounder.VectorizedLinearSearchRounder(values),
            new Rounder.EytzingerSearchRounder(values),
            new Rounder.VectorizedEytzingerSearchRounder(values),
            new Rounder.UnrolledEytzingerSearchRounder(values),
            new Rounder.HybridSearchRounder(values),
            new Rounder.BtreeSearchRounder(values),
        };

        for (int i = 0; i < 1000000; i++) {
            long key = (Math.abs(random.nextLong()) % values[values.length - 1]) + 10;
            long expected = impls[0].round(key);
            for (Rounder rounder : impls) {
                assertEquals(expected, rounder.round(key));
            }
        }
    }
}
