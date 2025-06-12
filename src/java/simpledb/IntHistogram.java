package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int[] hist;
    private int min;
    private int max;
    private int buckets;
    private int width;
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
    	this.buckets = Math.min(buckets, max - min + 1);
        this.hist = new int[this.buckets];
        this.min = min;
        this.max = max;
        this.width = (int) (Math.ceil((double) (max - min) / buckets));
        this.totalValues = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	if (v < min || v > max) {
            return;
        }
        int id = Math.min((v - min) / width, buckets - 1);
        hist[id]++;
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
        if (totalValues == 0) {
            return 0.0;
        }
        double selectivity = 0.0;
        int bucketIndex = Math.min(Math.max((v - min) / width, 0), buckets - 1);
    	switch (op){
            case GREATER_THAN:
                if (v < min){
                    return 1.0;
                }
                if (v >= max){
                    return 0.0;
                }
                int right = (min + (bucketIndex + 1) * width - 1);
                double bucketFraction = (double) (right - v) / width;
                selectivity += bucketFraction * hist[bucketIndex];

                for (int i = bucketIndex + 1; i < buckets; i++) {
                    selectivity += hist[i];
                }
                return selectivity / totalValues;

            case LESS_THAN:
                if (v <= min){
                    return 0.0;
                }
                if (v > max){
                    return 1.0;
                }

                int left = (min + bucketIndex * width);
                double leftFraction = (double) (v - left) / width;
                selectivity += leftFraction * hist[bucketIndex];

                for (int i = 0; i < bucketIndex; i++) {
                    selectivity += hist[i];
                }
                return selectivity / totalValues;

            case LESS_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.LESS_THAN, v + 1);

            case GREATER_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.GREATER_THAN, v - 1);

            case EQUALS:
                if (v < min || v > max) {
                    return 0.0;
                }
                return (double) hist[bucketIndex] / width / totalValues;

            case NOT_EQUALS:
                return 1.0 - estimateSelectivity(Predicate.Op.EQUALS, v);
            
            default:
                throw new IllegalArgumentException("Unsupported operator: " + op);
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
        if (totalValues == 0) {
            return 0.0;
        }
        double sum = 0.0;
        for (int i = 0; i < buckets; i++) {
            double height = hist[i];
            sum += (height / width) / totalValues;
        }
        return sum / buckets;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("IntHistogram: \n");
        for (int i = 0; i < hist.length; i++) {
            sb.append(String.format("Bucket %d: count = %d\n", i, hist[i]));
        }
        return sb.toString();
    }
}
