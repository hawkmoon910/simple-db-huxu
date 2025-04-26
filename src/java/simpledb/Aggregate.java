package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator child;

    private int afield;

    private int gfield;

    private Aggregator.Op aop;

    private Aggregator aggregator;

    private OpIterator aggregatorIterator;

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
	    this.child = child;
        this.afield =afield;
        this.gfield = gfield;
        this.aop = aop;
        this.td = child.getTupleDesc();
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	    return this.gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     * */
    public String groupFieldName() {
	    if (gfield == Aggregator.NO_GROUPING) {
            return null;
        }
        return this.td.getFieldName(gfield);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	    return this.afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
        if (gfield == Aggregator.NO_GROUPING) {
            return null;
        }
        return this.td.getFieldName(gfield);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
        return this.aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
	    super.open();
        child.open();

        Type gType = gfield == Aggregator.NO_GROUPING ? null : this.td.getFieldType(gfield);
        Type aType = this.td.getFieldType(afield);

        if (aType == Type.INT_TYPE) {
            aggregator = new IntegerAggregator(gfield, gType, afield, aop);
        } else if (aType == Type.STRING_TYPE) {
            aggregator = new StringAggregator(gfield, gType, afield, aop);
        } else {
            throw new IllegalArgumentException("Unsupported field type for aggregation.");
        }

        while (child.hasNext()) {
            aggregator.mergeTupleIntoGroup(child.next());
        }

        aggregatorIterator = aggregator.iterator();
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
        if (aggregatorIterator == null || !aggregatorIterator.hasNext()) {
            return null;
        }
        return aggregatorIterator.next();
    }

    public void rewind() throws DbException, TransactionAbortedException {
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
        Type aType = child.getTupleDesc().getFieldType(afield);
        String aName = nameOfAggregatorOp(aop) + " (" + child.getTupleDesc().getFieldName(afield) + ")";

        if (gfield == Aggregator.NO_GROUPING) {
            return new TupleDesc(new Type[]{aType}, new String[]{aName});
        } else {
            Type gType = child.getTupleDesc().getFieldType(gfield);
            String gName = child.getTupleDesc().getFieldName(gfield);
            return new TupleDesc(new Type[]{gType, aType}, new String[]{gName, aName});
        }
    }

    public void close() {
	    super.close();
        child.close();
        if (aggregatorIterator != null) {
            aggregatorIterator.close();
        }
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
	    this.child = children[0];
    }
    
}
