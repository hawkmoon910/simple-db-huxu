package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    // The transaction this delete runs in
    private final TransactionId t;
    // The child operator from which to read tuples for deletion
    private OpIterator child;
    // Tracks if delete has happened yet
    private boolean deleteOrNot;
    // TupleDesc describing the output of this operator
    private final TupleDesc resultDesc;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // Stores transaction
        this.t = t;
        // Stores child operator
        this.child = child;
        // Set delete flag to false
        this.deleteOrNot = false;
        // Result contains one INT field
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
        // Reset delete flag
        deleteOrNot = false;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // If delete already happened, return null
        if (deleteOrNot) {
            return null;
        }
        // Counter for deleted tuples
        int count = 0;
        // Get the global buffer pool instance
        BufferPool bufferPool = Database.getBufferPool();
        // Loop through all tuples from child operator
        while (child.hasNext()) {
            // Get next tuple
            Tuple nextTuple = child.next();
            try {
                // Delete tuple into the table using BufferPool
                bufferPool.deleteTuple(t, nextTuple);
                // Increment deleted tuple count
                count++;
            } catch (IOException e) {
                // Throw exception if delete error
                throw new DbException("IO error during delete: " + e.getMessage());
            }
        }
        // Set flag so we don’t delete again
        deleteOrNot = true;
        // Create a new tuple containing number of deleted tuples
        Tuple result = new Tuple(resultDesc);
        // Set the first field to the number of delete tuples
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
