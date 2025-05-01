package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    // The OpIterator that is feeding us tuples
    private OpIterator child;
    // The column over which we are computing an aggregate
    private int afield;
    // The column over which we are grouping the result, or -1 if there is no grouping
    private int gfield;
    // The aggregation operator to use
    private Aggregator.Op aop;
    // Aggregator used to compute the result
    private Aggregator aggregator;
    // Iterator over the aggregate results
    private OpIterator aggregatorIterator;
    // TupleDesc of the child input
    private TupleDesc td;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The OpIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        // Stores OpIterator child
	    this.child = child;
        // Stores column for computing aggregate
        this.afield = afield;
        // Stores column for grouping result
        this.gfield = gfield;
        // Stores aggregation operator
        this.aop = aop;
        // Cache TupleDesc for field info
        this.td = child.getTupleDesc();
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
        // Returns groupby field index
	    return this.gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     * */
    public String groupFieldName() {
        // If no grouping, return null
	    if (gfield == Aggregator.NO_GROUPING) {
            return null;
        }
        // Returns the name of the group-by field
        return this.td.getFieldName(gfield);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
        // Returns the aggregate field
	    return this.afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
        // If no grouping, return null
        if (gfield == Aggregator.NO_GROUPING) {
            return null;
        }
        // Returns name of the aggregated field
        return this.td.getFieldName(gfield);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
        // Returns the aggregate operator
        return this.aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        // Returns string representation (name) of the aggregate operator
	    return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
        // Call parent open
	    super.open();
        // Open the child operator
        child.open();
        // Determine type of the group-by field (or null if no grouping)
        Type gType = gfield == Aggregator.NO_GROUPING ? null : this.td.getFieldType(gfield);
        // Determine type of aggregate field
        Type aType = this.td.getFieldType(afield);
        // Create appropriate aggregator based on field type
        if (aType == Type.INT_TYPE) {
            // For integers
            aggregator = new IntegerAggregator(gfield, gType, afield, aop);
        } else if (aType == Type.STRING_TYPE) {
            // For Strings
            aggregator = new StringAggregator(gfield, gType, afield, aop);
        } else {
            // Unsupported type, throw exception
            throw new IllegalArgumentException("Unsupported field type.");
        }
        // Process each tuple from input and merge into aggregator
        while (child.hasNext()) {
            aggregator.mergeTupleIntoGroup(child.next());
        }
        // Get iterator for aggregated results
        aggregatorIterator = aggregator.iterator();
        // Open iterator to start reading results
        aggregatorIterator.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // If no results left or iterator is closed, return null to stop
        if (aggregatorIterator == null || !aggregatorIterator.hasNext()) {
            return null;
        }
        // Return next result tuple
        return aggregatorIterator.next();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // If iterator is opened, rewind to beginning
	    if (aggregatorIterator != null) {
            aggregatorIterator.rewind();
        }
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        // Get type of aggregate field
        Type aType = child.getTupleDesc().getFieldType(afield);
        // Get name of aggregate field
        String aName = nameOfAggregatorOp(aop) + " (" + child.getTupleDesc().getFieldName(afield) + ")";
        // If no grouping
        if (gfield == Aggregator.NO_GROUPING) {
            // Return single aggregate column
            return new TupleDesc(new Type[]{aType}, new String[]{aName});
        } else {
            // With grouping
            // Get type of group field
            Type gType = child.getTupleDesc().getFieldType(gfield);
            // Get name of group field
            String gName = child.getTupleDesc().getFieldName(gfield);
            // Return columns with group fields
            return new TupleDesc(new Type[]{gType, aType}, new String[]{gName, aName});
        }
    }

    public void close() {
        // Call parent close
	    super.close();
        // Close the child operator
        child.close();
        // Close the iterator if iterator is open
        if (aggregatorIterator != null) {
            aggregatorIterator.close();
        }
    }

    @Override
    public OpIterator[] getChildren() {
        // Return child in array form
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // Update the child operator
	    this.child = children[0];
    }
    
}
