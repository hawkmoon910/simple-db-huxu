package simpledb;

import java.util.*;
import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    // The transaction running the insert
    private final TransactionId t;
    // The child operator from which to read tuples to be inserted
    private OpIterator child;
    // The table in which to insert tuples
    private final int tableId;
    // Tracks if insert has happened yet
    private boolean insertOrNot;
    // TupleDesc describing the output of this operator
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
        // Stores transaction
        this.t = t;
        // Stores child operator
        this.child = child;
        // Stores table id
        this.tableId = tableId;
        // Set insert flag to false
        this.insertOrNot = false;

        // Fetch the tuple description of the target table
        TupleDesc tableTd = Database.getCatalog().getTupleDesc(tableId);
        // Check if the tuple format of child matches with the target table
        if (!child.getTupleDesc().equals(tableTd)) {
            // Throw exception
            throw new DbException("TupleDesc mismatch between child and target table.");
        }

        // Result tuple will contain one field: number of inserted tuples
        this.resultDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
    }

    public TupleDesc getTupleDesc() {
        // Returns the tuple desc
        return this.resultDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // Call parent open
        super.open();
        // Open the child operator
        child.open();
    }

    public void close() {
        // Call parent close
        super.close();
        // Close the child operator
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // Rewind the child operator
        child.rewind();
        // Reset insert flag
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
        // If insert already happened, return null
        if (insertOrNot) {
            return null;
        }
        // Counter for inserted tuples
        int count = 0;
        // Get the global buffer pool instance
        BufferPool bufferPool = Database.getBufferPool();
        // Loop through all tuples from child operator
        while (child.hasNext()) {
            // Get next tuple
            Tuple nextTuple = child.next();
            try {
                // Insert tuple into the table using BufferPool
                bufferPool.insertTuple(t, tableId, nextTuple);
                // Increment inserted tuple count
                count++;
            } catch (IOException e) {
                // Throw exception if insert error
                throw new DbException("IO error during insert: " + e.getMessage());
            }
        }
        // Set flag so we don’t insert again
        insertOrNot = true;
        // Create a new tuple to hold the result count
        Tuple result = new Tuple(resultDesc);
        // Set the first field to the number of inserted tuples
        result.setField(0, new IntField(count));
        // Return result
        return result;
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
