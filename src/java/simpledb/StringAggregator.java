package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    // The 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
    private int gbfield;
    // The type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
    private Type gbfieldtype;
    // The 0-based index of the aggregate field in the tuple
    private int afield;
    // The aggregation operator -- only supports COUNT
    private Op what;
     // Map to track count per group, if no grouping, the key is null
    private Map<Field, Integer> groupCounts;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if (what != Op.COUNT) {
            throw new IllegalArgumentException("StringAggregator only supports COUNT");
        }
        // Stores 0-based index of group-by field
        this.gbfield = gbfield;
        // Stores type of group-by field
        this.gbfieldtype = gbfieldtype;
        // Stores 0-based index of aggregate field
        this.afield = afield;
        // Stores aggregation operator
        this.what = what;
        // Initialize empty count map
        this.groupCounts = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // Determine the group field, null if no grouping
        Field groupField = (gbfield == NO_GROUPING) ? null : tup.getField(gbfield);
        // Increment the count for the group using merge, adds 1 or sums with existing
        groupCounts.merge(groupField, 1, Integer::sum);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // New array list of tuples called results
        List<Tuple> results = new ArrayList<>();
        // Create the appropriate TupleDesc (type schema) depending on grouping
        TupleDesc td = (gbfield == NO_GROUPING)
                // One INT field for count
                ? new TupleDesc(new Type[] { Type.INT_TYPE })
                // Group field + count
                : new TupleDesc(new Type[] { gbfieldtype, Type.INT_TYPE });
        // Construct tuples from groupCounts
        for (Map.Entry<Field, Integer> entry : groupCounts.entrySet()) {
            // Make result tuple
            Tuple result = new Tuple(td);
            // If no grouping
            if (gbfield == NO_GROUPING) {
                // Set count in only field
                result.setField(0, new IntField(entry.getValue()));
            // With grouping
            } else {
                // set group value and count
                result.setField(0, entry.getKey());
                result.setField(1, new IntField(entry.getValue()));
            }
            // Add result to results list
            results.add(result);
        }
        // Return TupleIterator to iterate over the result tuples
        return new TupleIterator(td, results);
    }

}
