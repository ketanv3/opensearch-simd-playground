package org.opensearch;

import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorSpecies;

public class VectorUtil {
    public static final VectorSpecies<Long> LONG_SPECIES = LongVector.SPECIES_PREFERRED;
    public static final int LONG_LANES = LONG_SPECIES.length();
    public static final int LONG_SHIFT = log2(LONG_LANES);

    private static int log2(int num) {
        if ((num & (num - 1)) != 0) {
            throw new IllegalArgumentException("must be a power of two: " + num);
        }

        int pow = 0;
        while (num > 1) {
            pow += 1;
            num >>= 1;
        }

        return pow;
    }
}
