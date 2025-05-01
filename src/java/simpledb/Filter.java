package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;
    // The predicate used to filter tuples
    private final Predicate p;
    // The child operator which tuples are read from
    private OpIterator child;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        // Store predicate
        this.p = p;
        // Store child operator
        this.child = child;
    }

    public Predicate getPredicate() {
        // Returns predicate p
        return this.p;
    }

    public TupleDesc getTupleDesc() {
        // Returns the TupleDesc of the child operator
        return this.child.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // Call open on the parent class
        super.open();
        // Open child operator
        this.child.open();
    }

    public void close() {
        // Call close on the parent class
        super.close();
        // Close child operator
        this.child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // Rewind child operator
        this.child.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // Iterate over all tuples from child operator
        while (this.child.hasNext()) {
            // Get next tuple
        	Tuple tup = this.child.next();
            // Check if tuple satisfies predicate
        	if (p.filter(tup)) {
                // Returns tuple if it does
        		return tup;
        	}
        }
        // No matches, return null
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // Return an array with one child
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // Checks for amount of children, throws error if null or not one
        if (children == null || children.length != 1) {
            throw new IllegalArgumentException("Only one child allowed");
        }
        // Set child operator to given one
        this.child = children[0];
    }

}
