package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private final TransactionId t;

    private OpIterator child;

    private boolean deleteOrNot;

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
        this.t = t;
        this.child = child;
        this.deleteOrNot = false;
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
        if (deleteOrNot) {
            return null;
        }

        BufferPool bufferPool = Database.getBufferPool();
        int count = 0;

        while (child.hasNext()) {
            Tuple temp = child.next();
            try {
                bufferPool.deleteTuple(t, temp);
                count++;
            } catch (IOException e) {
                throw new DbException("IO error during delete: " + e.getMessage());
            }
        }

        deleteOrNot = true;

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
            throw new IllegalArgumentException("Delete operator expects one child.");
        }
        this.child = children[0];
    }

}
