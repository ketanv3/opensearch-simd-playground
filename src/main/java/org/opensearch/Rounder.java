package org.opensearch;

import static org.opensearch.VectorUtil.LONG_LANES;
import static org.opensearch.VectorUtil.LONG_SHIFT;
import static org.opensearch.VectorUtil.LONG_SPECIES;

import java.util.Arrays;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.Vector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;

/**
 * Rounder rounds-down the given key. In other words, it returns the greatest lower bound of the
 * given key in a sorted list of values.
 */
@FunctionalInterface
public interface Rounder {
    long round(long key);

    /** Uses naive binary search to find the round-down point. */
    class BinarySearchRounder implements Rounder {
        private final long[] values;

        public BinarySearchRounder(long[] values) {
            this.values = values;
        }

        @Override
        public long round(long key) {
            int idx = Arrays.binarySearch(values, key);
            if (idx < 0) idx = -2 - idx;
            return values[idx];
        }
    }

    /** Uses naive linear search to find the round-down point. */
    class LinearSearchRounder implements Rounder {
        private final long[] values;

        public LinearSearchRounder(long[] values) {
            this.values = values;
        }

        @Override
        public long round(long key) {
            int i = 0;
            for (; i < values.length; i++) {
                if (values[i] > key) return values[i - 1];
            }
            return values[i - 1];
        }
    }

    /**
     * Uses vectorized linear search to find the round-down point. The values array is padded with
     * {@code Long.MAX_VALUE} so that its length is always an exact multiple of the vector lanes.
     */
    class VectorizedLinearSearchRounder implements Rounder {
        private final long[] values;

        public VectorizedLinearSearchRounder(long[] values) {
            int alignedLength = ((values.length + LONG_LANES - 1) / LONG_LANES) * LONG_LANES;
            this.values = new long[alignedLength];
            System.arraycopy(values, 0, this.values, 0, values.length);
            Arrays.fill(this.values, values.length, alignedLength, Long.MAX_VALUE);
        }

        @Override
        public long round(long key) {
            Vector<Long> keyVector = LongVector.broadcast(LONG_SPECIES, key);
            int idx = 0;
            int max = LONG_SPECIES.loopBound(values.length);
            for (; idx < max; idx += LONG_LANES) {
                Vector<Long> valuesVector = LongVector.fromArray(LONG_SPECIES, values, idx);
                VectorMask<Long> mask = valuesVector.compare(VectorOperators.GT, keyVector);
                int off = mask.firstTrue();
                if (off < LONG_LANES) return values[idx + mask.firstTrue() - 1];
            }
            return values[idx - 1];
        }
    }

    /** Uses binary search with Eytzinger layout to find the round-down point. */
    class EytzingerSearchRounder implements Rounder {
        private final long[] values;

        public EytzingerSearchRounder(long[] values) {
            this.values = new long[values.length + 1];
            build(values, 0, this.values, 1);
        }

        private static int build(long[] src, int i, long[] dst, int j) {
            if (j < dst.length) {
                i = build(src, i, dst, j << 1);
                dst[j] = src[i++];
                i = build(src, i, dst, (j << 1) + 1);
            }
            return i;
        }

        @Override
        public long round(long key) {
            int idx = 1, res = -1;
            while (idx < values.length) {
                if (values[idx] <= key) {
                    res = idx;
                    idx = (idx << 1) + 1;
                } else {
                    idx <<= 1;
                }
            }
            return values[res];
        }
    }

    /**
     * Uses vectorized binary search with Eytzinger layout to find the round-down point. It is very
     * similar to {@link BtreeSearchRounder}, but here, the tree is guaranteed to be <a
     * href="https://www.baeldung.com/cs/full-vs-complete-vs-perfect-tree#complete-binary-tree">complete</a>.
     *
     * <p>The rationale of having a complete tree is that, given the nature of timeseries data where
     * velocity increases over time and a greater timestamp is more frequently accessed, it will be
     * laid out in the second-last level, thus, reducing one extra branch.
     */
    class VectorizedEytzingerSearchRounder implements Rounder {
        private final long[] values;

        public VectorizedEytzingerSearchRounder(long[] values) {
            int alignedLength = 1 + ((values.length + LONG_LANES - 1) / LONG_LANES) * LONG_LANES;
            this.values = new long[alignedLength];
            Arrays.fill(this.values, values.length + 1, this.values.length, Long.MAX_VALUE);
            build(values, 0, this.values, 1, values.length + 1);
        }

        private static int build(long[] src, int i, long[] dst, int j, int max) {
            if (j < max) {
                int k = j + (j << VectorUtil.LONG_SHIFT);
                i = build(src, i, dst, k, max);
                for (int l = 0; l < LONG_LANES; l++) {
                    if (j + l < max) dst[j + l] = src[i++];
                    k += LONG_LANES;
                    i = build(src, i, dst, k, max);
                }
            }
            return i;
        }

        @Override
        public long round(long key) {
            Vector<Long> keyVector = LongVector.broadcast(LONG_SPECIES, key);

            int idx = 1, res = 0;

            while (idx < values.length) {
                Vector<Long> valuesVector = LongVector.fromArray(LONG_SPECIES, values, idx);
                VectorMask<Long> mask = valuesVector.compare(VectorOperators.GT, keyVector);
                int off = mask.firstTrue();
                int pos = idx + off;
                if (off != 0) res = pos;
                idx += pos << VectorUtil.LONG_SHIFT;
            }

            return values[res - 1];
        }
    }

    /**
     * Exactly like {@link VectorizedEytzingerSearchRounder}, except, uses an unrolled {@link
     * UnrolledEytzingerSearchRounder::firstTrue} method aligned to the cache-line size (usually
     * 64-bytes) instead of the vector lanes.
     */
    class UnrolledEytzingerSearchRounder implements Rounder {
        private static final int LANES = 8;
        private static final int SHIFT = 3;

        private final long[] values;

        public UnrolledEytzingerSearchRounder(long[] values) {
            int alignedLength = 1 + ((values.length + LANES - 1) / LANES) * LANES;
            this.values = new long[alignedLength];
            Arrays.fill(this.values, values.length + 1, this.values.length, Long.MAX_VALUE);
            build(values, 0, this.values, 1, values.length + 1);
        }

        private static int build(long[] src, int i, long[] dst, int j, int max) {
            if (j < max) {
                int k = j + (j << SHIFT);
                i = build(src, i, dst, k, max);
                for (int l = 0; l < LANES; l++) {
                    if (j + l < max) dst[j + l] = src[i++];
                    k += LANES;
                    i = build(src, i, dst, k, max);
                }
            }
            return i;
        }

        @Override
        public long round(long key) {
            int idx = 1, res = 0;

            while (idx < values.length) {
                int off = firstTrue(values, idx, key);
                int pos = idx + off;
                if (off != 0) res = pos;
                idx += pos << SHIFT;
            }

            return values[res - 1];
        }

        private static int firstTrue(long[] values, int idx, long key) {
            // Use with 8 lanes:
            if (values[idx + 3] > key) {
                if (values[idx + 1] > key) {
                    return (values[idx] > key) ? 0 : 1;
                } else {
                    return (values[idx + 2] > key) ? 2 : 3;
                }
            } else if (values[idx + 5] > key) {
                return (values[idx + 4] > key) ? 4 : 5;
            } else if (values[idx + 7] > key) {
                return (values[idx + 6] > key) ? 6 : 7;
            } else {
                return 8;
            }

            /* Use with 4 lanes:
            if (values[idx + 1] > key) {
                return (values[idx] > key) ? 0 : 1;
            } else if (values[idx + 3] > key) {
                return (values[idx + 2] > key) ? 2 : 3;
            }
            return 4;
            */

            /* (Hopefully) branch-less and unrolled equivalent:
            int mask = 1 << LANES;
            for (int l = 0; l < LANES; l++) {
                mask |= ((values[idx + l] > key) ? 1 : 0) << l;
            }
            return Integer.numberOfTrailingZeros(mask);
            */
        }
    }

    /**
     * Uses a combination of binary search and linear search to find the round-down point. It first
     * uses binary search to split the search space until it's small enough to be searched by linear
     * search.
     */
    class HybridSearchRounder implements Rounder {
        private final long[] values;
        private final int maxSplits;

        public HybridSearchRounder(long[] values) {
            this.values = values;
            int n = values.length;
            int s = 0;
            while (n > 64) {
                s++;
                n /= 2;
            }
            maxSplits = s;
        }

        @Override
        public long round(long key) {
            int low = 0, high = values.length - 1;

            for (int s = 0; s < maxSplits; s++) {
                int mid = (low + high) >> 1;
                if (values[mid] <= key) {
                    low = mid;
                } else {
                    high = mid - 1;
                }
            }

            for (int idx = low; idx <= high; idx++) {
                if (values[idx] > key) return values[idx - 1];
            }

            return values[high];
        }
    }

    /** Uses vectorized B-tree search to find the round-down point. */
    class BtreeSearchRounder implements Rounder {
        private final long[] values;

        public BtreeSearchRounder(long[] values) {
            int len = ((values.length + LONG_LANES - 1) / LONG_LANES) * LONG_LANES;
            this.values = new long[len + 1];
            build(values, 0, this.values, 1);
        }

        private static int build(long[] src, int i, long[] dst, int j) {
            if (j < dst.length) {
                for (int k = 0; k < LONG_LANES; k++) {
                    i = build(src, i, dst, j + ((j + k) << LONG_SHIFT));
                    dst[j + k] = (i < src.length) ? src[i++] : Long.MAX_VALUE;
                    // Uncomment to create a complete tree instead.
                    // dst[j + k] = (j + k < src.length + 1) ? src[i++] : Long.MAX_VALUE;
                }
                i = build(src, i, dst, j + ((j + LONG_LANES) << LONG_SHIFT));
            }
            return i;
        }

        @Override
        public long round(long key) {
            Vector<Long> k = LongVector.broadcast(LONG_SPECIES, key);
            int i = 1, res = 0;
            while (i < values.length) {
                Vector<Long> v = LongVector.fromArray(LONG_SPECIES, values, i);
                int j = i + v.compare(VectorOperators.GT, k).firstTrue();
                res = (j > i) ? j : res;
                i += j << LONG_SHIFT;
            }
            return values[res - 1];
        }
    }
}
