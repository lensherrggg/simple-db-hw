package simpledb;

import java.util.ArrayList;
import java.util.List;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private class Bucket {
        private int left;
        private int right;
        private int cnt;

        public Bucket(int left, int right) {
            this.left = left;
            this.right = right;
            cnt = 0;
        }

        public int getLeft() {
            return left;
        }

        public int getRight() {
            return right;
        }

        public int getCnt() {
            return cnt;
        }

        public void setCnt(int cnt) {
            this.cnt = cnt;
        }

        public int getWidth() {
            // don't forget to plus one
            return right - left + 1;
        }

        public void increase() {
            cnt++;
        }

        @Override
        public String toString() {
            return "<" + left + "," + right + ">:" + cnt;
        }
    }

    private int numBuckets;
    private int minValue;
    private int maxValue;
    private double width;
    private List<Bucket> buckets;
    private int nTup;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// Done
        numBuckets = buckets;
        minValue = min;
        maxValue = max;
        this.buckets = new ArrayList<>();
        // don't forget to plus one
        this.width = (1.0 + max - min) / buckets;
        for (int i = 0; i < numBuckets; i++) {
            int left = (int) Math.ceil(min + i * width);
            int right = (int) Math.ceil(min + (i + 1) * width) - 1;
            if (right < left) {
                right = left;
            }

            this.buckets.add(new Bucket(left, right));
        }
    }

    private int getBucketIndex(int v) {
        return (int) ((v - minValue) / width);
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// Done
//        if (v < minValue || v > maxValue) {
//            throw new IllegalArgumentException("Value out of range");
//        }
        int idx = getBucketIndex(v);
        buckets.get(idx).increase();
        nTup++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	// Done
        int index;
        double sum;
        Bucket bucket;

        switch (op) {
            case EQUALS:
                index = getBucketIndex(v);
                if (index < 0 || index >= numBuckets) {
                    return 0.0;
                }
                bucket = buckets.get(index);
                // (h / w) / nTup
                return (1.0 * bucket.getCnt() / bucket.getWidth()) / nTup;
            case GREATER_THAN:
                index = getBucketIndex(v);
                if (index < 0) {
                    return 1.0;
                }
                if (index >= numBuckets) {
                    return 0.0;
                }
                bucket = buckets.get(index);
                sum = 1.0 * bucket.getCnt() * (bucket.getRight() - v) / bucket.getWidth();
                for (int i = index + 1; i < numBuckets; i++) {
                    sum += buckets.get(i).getCnt();
                }
                return sum / nTup;
            case LESS_THAN:
                index = getBucketIndex(v);
                if (index < 0) {
                    return 0.0;
                }
                if (index >= numBuckets) {
                    return 1.0;
                }
                bucket = buckets.get(index);
                sum = 1.0 * bucket.getCnt() * (v - bucket.getLeft()) / bucket.getWidth();
                for (int i = index - 1; i >= 0; i--) {
                    sum += buckets.get(i).getCnt();
                }
                return sum / nTup;
            case GREATER_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.GREATER_THAN, v - 1);
            case NOT_EQUALS:
                return 1 - estimateSelectivity(Predicate.Op.EQUALS, v);
            case LESS_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.LESS_THAN, v + 1);
            default:
                throw new UnsupportedOperationException();
        }
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // Done
        int cnt = 0;
        for (Bucket b : buckets) {
            cnt += b.getCnt();
        }
        return 1.0 * cnt / numBuckets;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // Done
        StringBuilder res = new StringBuilder("|| ");
        for (Bucket b : buckets) {
            res.append(b);
            res.append(" || ");
        }
        return res.toString();
    }
}
