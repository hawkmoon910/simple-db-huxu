package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    // The 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
    private final int gbfield;
    // The type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
    private final Type gbfieldtype;
    // The 0-based index of the aggregate field in the tuple
    private final int afield;
    // The aggregation operator
    private final Op what;
    // A map from group field to a list of integer values to aggregate
    private final Map<Field, List<Integer>> groups;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // Stores 0-based index of group-by field
        this.gbfield = gbfield;
        // Stores type of group-by field
        this.gbfieldtype = gbfieldtype;
        // Stores 0-based index of aggregate field
        this.afield = afield;
        // Stores aggregation operator
        this.what = what;
        // Initialize empty group map
        this.groups = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // Determine group key, null if no grouping
        Field groupKey = (gbfield == NO_GROUPING) ? null : tup.getField(gbfield);
        // Extract aggregate field value
        IntField aVal = (IntField) tup.getField(afield);
        // Append value to the list for this group, initialize list if needed
        groups.computeIfAbsent(groupKey, k -> new ArrayList<>()).add(aVal.getValue());
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // New array list of tuples called results
        List<Tuple> results = new ArrayList<>();
        // Create the tuple descriptor, one or two fields depending on grouping
        TupleDesc td;
        if (gbfield == NO_GROUPING) {
            td = new TupleDesc(new Type[]{Type.INT_TYPE});
        } else {
            td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
        }
        // Iterate over each group and compute the aggregate
        for (Map.Entry<Field, List<Integer>> entry : groups.entrySet()) {
            // Group key
            Field group = entry.getKey();
            // List of values in the group
            List<Integer> values = entry.getValue();
            // Integer to hold the computed aggregate
            int aggregateVal;
            // Compute aggregate based on operation
            switch (what) {
                case COUNT:
                    aggregateVal = values.size();
                    break;
                case SUM:
                    aggregateVal = values.stream().mapToInt(i -> i).sum();
                    break;
                case AVG:
                    aggregateVal = values.stream().mapToInt(i -> i).sum() / values.size();
                    break;
                case MIN:
                    aggregateVal = values.stream().mapToInt(i -> i).min().orElse(0);
                    break;
                case MAX:
                    aggregateVal = values.stream().mapToInt(i -> i).max().orElse(0);
                    break;
                default:
                    // Throw exception if an unsupported operator is used
                    throw new UnsupportedOperationException("Unsupported aggregate operator: " + what);
            }

            // Make result tuple
            Tuple result = new Tuple(td);
            if (gbfield == NO_GROUPING) {
                result.setField(0, new IntField(aggregateVal));
            } else {
                result.setField(0, group);
                result.setField(1, new IntField(aggregateVal));
            }
            // Add result to results list
            results.add(result);
        }
        // Return TupleIterator to iterate over the result tuples
        return new TupleIterator(td, results);
    }

}
