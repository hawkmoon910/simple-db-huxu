package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
    // Array to count values per bin
    private int[] hist;
    // Minimum value
    private int min;
    // Maximum value
    private int max;
    // Number of bins in the histogram
    private int buckets;
    // Width of each bin
    private int width;
    // Total number of values added
    private int totalValues;

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
        // Set buckets to computed number of buckets
    	this.buckets = Math.min(buckets, max - min + 1);
        // Initialize histogram array
        this.hist = new int[this.buckets];
        // Set min to minimum value
        this.min = min;
        // Set max to maximum value
        this.max = max;
        // Set width to computed bin width
        this.width = (int) (Math.ceil((double) (max - min) / buckets));
        // Initialize totalValues to zero
        this.totalValues = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        // Ignore out-of-range values
    	if (v < min || v > max) {
            return;
        }
        // Determine correct bin (id)
        int id = Math.min((v - min) / width, buckets - 1);
        // Increment count in id
        hist[id]++;
        // Increment total value count
        totalValues++;
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
        // If there are no values in the histogram, selectivity is zero
        if (totalValues == 0) {
            return 0.0;
        }
        // If value is less than the histogram's minimum
        if (v < min) {
            switch (op) {
                case GREATER_THAN:
                // Everything is greater than v
                case GREATER_THAN_OR_EQ:
                    return 1.0;
                case LESS_THAN:
                case LESS_THAN_OR_EQ:
                // Nothing is less than, equal to or less-or-equal to v
                case EQUALS:
                    return 0.0;
                // Everything is not equal to v
                case NOT_EQUALS:
                    return 1.0;
            }
        }
        // If value is greater than the histogram's maximum
        if (v > max) {
            switch (op) {
                case LESS_THAN:
                // Everything is less than v
                case LESS_THAN_OR_EQ:
                    return 1.0;
                case GREATER_THAN:
                case GREATER_THAN_OR_EQ:
                // Nothing is greater than, equal to or greater-or-equal to v
                case EQUALS:
                    return 0.0;
                // Everything is not equal to v
                case NOT_EQUALS:
                    return 1.0;
            }
        }
        // Compute the index of the bucket where v falls
        int bucketIndex = Math.min((v - min) / width, buckets - 1);
        // Count for estimated selectivity
        double selectivity = 0.0;
        // Handle different predicate operations
        switch (op) {
            case GREATER_THAN: {
                // Compute the right end of the bucket
                int right = min + (bucketIndex + 1) * width - 1;
                // Fraction of values in the current bucket greater than v
                double bucketFraction = (double) (right - v) / width;
                // Add partial contribution of the current bucket to selectivity
                selectivity += bucketFraction * hist[bucketIndex];
                // Add full contributions of all buckets to the right
                for (int i = bucketIndex + 1; i < buckets; i++) {
                    selectivity += hist[i];
                }
                break;
            }

            case LESS_THAN: {
                // Compute the left end of the bucket
                int left = min + bucketIndex * width;
                // Fraction of values in the current bucket less than v
                double bucketFraction = (double) (v - left) / width;
                // Add partial contribution of the current bucket to selectivity
                selectivity += bucketFraction * hist[bucketIndex];
                // Add full contributions of all buckets to the left
                for (int i = 0; i < bucketIndex; i++) {
                    selectivity += hist[i];
                }
                break;
            }

            case LESS_THAN_OR_EQ:
                // Convert to LESS_THAN of v+1 for estimation
                return estimateSelectivity(Predicate.Op.LESS_THAN, v + 1);

            case GREATER_THAN_OR_EQ:
                // Convert to GREATER_THAN of v-1 for estimation
                return estimateSelectivity(Predicate.Op.GREATER_THAN, v - 1);

            case EQUALS:
                // Return fraction of values in the bucket, normalized over bucket width and total count
                return (double) hist[bucketIndex] / width / totalValues;

            case NOT_EQUALS:
                // Selectivity of not equals is 1 - selectivity of equals
                return 1.0 - estimateSelectivity(Predicate.Op.EQUALS, v);

            default:
                // Throw an error for unsupported operators
                throw new IllegalArgumentException("Unsupported operator: " + op);
        }
        // Normalize selectivity divided by total number of values in the histogram
        return selectivity / totalValues;
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
        // If there are no values in the histogram, selectivity is zero
        if (totalValues == 0) {
            return 0.0;
        }
        // Count for sum
        double sum = 0.0;
        // Loop through each bucket in the histogram
        for (int i = 0; i < buckets; i++) {
            // Get the height (entries) in the current bucket
            double height = hist[i];
            // Estimate the selectivity for a typical value in this bucke
            sum += (height / width) / totalValues;
        }
        // Return the average selectivity across all buckets
        return sum / buckets;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // Create string builder
        StringBuilder sb = new StringBuilder();
        // Print IntHistogram
        sb.append("IntHistogram: \n");
        // Loop through length of the histogram
        for (int i = 0; i < hist.length; i++) {
            // Prints bucket and count info
            sb.append(String.format("Bucket %d: count = %d\n", i, hist[i]));
        }
        // Return formatted string
        return sb.toString();
    }
}
