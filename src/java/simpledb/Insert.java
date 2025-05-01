package simpledb;

import java.util.*;
import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private final TransactionId t;

    private OpIterator child;

    private final int tableId;

    private boolean insertOrNot;

    private final TupleDesc resultDesc;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        this.t = t;
        this.child = child;
        this.tableId = tableId;
        this.insertOrNot = false;

        // Verify that the child's tuple desc matches the target table's tuple desc
        TupleDesc tableTd = Database.getCatalog().getTupleDesc(tableId);
        if (!child.getTupleDesc().equals(tableTd)) {
            throw new DbException("TupleDesc mismatch between child and target table.");
        }

        // Define the result tuple descriptor (a single INT field indicating number of inserted records)
        this.resultDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
    }

    public TupleDesc getTupleDesc() {
        return this.resultDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        child.open();
    }

    public void close() {
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
        insertOrNot = false;
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (insertOrNot) {
            return null;
        }

        int count = 0;
        BufferPool bufferPool = Database.getBufferPool();

        while (child.hasNext()) {
            Tuple temp = child.next();
            try {
                bufferPool.insertTuple(t, tableId, temp);
                count++;
            } catch (IOException e) {
                throw new DbException("IO error during insert: " + e.getMessage());
            }
        }

        insertOrNot = true;

        // Return a single-field tuple with the count of inserted records
        Tuple result = new Tuple(resultDesc);
        result.setField(0, new IntField(count));
        return result;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        if (children.length != 1) {
            throw new IllegalArgumentException("Expected exactly one child.");
        }
        this.child = children[0];
    }
}
