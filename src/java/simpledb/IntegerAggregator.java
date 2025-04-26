package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    
    private Type gbfieldtype;
    
    private int afield;
    
    private Op what;

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
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
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
        Field groupKey = (gbfield == NO_GROUPING) ? null : tup.getField(gbfield);
        IntField aVal = (IntField) tup.getField(afield);

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
        List<Tuple> results = new ArrayList<>();

        TupleDesc td;
        if (gbfield == NO_GROUPING) {
            td = new TupleDesc(new Type[]{Type.INT_TYPE});
        } else {
            td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
        }

        for (Map.Entry<Field, List<Integer>> entry : groups.entrySet()) {
            Field group = entry.getKey();
            List<Integer> values = entry.getValue();
            int aggregateVal;

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
                    throw new UnsupportedOperationException("Unsupported aggregate operator: " + what);
            }

            Tuple result = new Tuple(td);
            if (gbfield == NO_GROUPING) {
                result.setField(0, new IntField(aggregateVal));
            } else {
                result.setField(0, group);
                result.setField(1, new IntField(aggregateVal));
            }
            results.add(result);
        }

        return new TupleIterator(td, results);
    }

}
