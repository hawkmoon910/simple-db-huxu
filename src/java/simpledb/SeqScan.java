package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;

    // Transaction Id to scan
    private TransactionId tid;
    // Table Id to scan
    private int tableid;
    // Alias for name of fields
    private String tableAlias;
    // Iterator
    private DbFileIterator it;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // Intitalizes everything by setting all fields to the new ones, iterator it to null
        this.tid = tid;
        this.tableid = tableid;
        this.tableAlias = tableAlias;
        this.it = null;
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        // Returns name of table not alias
       return Database.getCatalog().getTableName(tableid);
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias()
    {
        // Returns alias of table
        return this.tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // Sets tableid and tableAlias to new ones and sets iterator it to null again
        this.tableid = tableid;
        this.tableAlias = tableAlias;
        this.it = null;
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    public void open() throws DbException, TransactionAbortedException {
        // Retrieve the DbFile for the table
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableid);
        // Create the iterator for it
        it = dbFile.iterator(tid);
        // Opens iterator it
        it.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // Gets schema for table
        TupleDesc original = Database.getCatalog().getTupleDesc(tableid);
        // Pre-allocate arrays for types and aliased field names
        Type[] types = new Type[original.numFields()];
        String[] fieldNames = new String[original.numFields()];

        // Checks all fields
        for (int i = 0; i < original.numFields(); i++) {
            // Gets type of field
            types[i] = original.getFieldType(i);
            // Gets name of field
            String fieldName = original.getFieldName(i);
            // Prefixs it with alias =
            fieldNames[i] = tableAlias + "." + fieldName;
        }

        // Returns the new TupleDesc (schema)
        return new TupleDesc(types, fieldNames);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // Returns true if iterator exists and there are more tuples, otherwise false
        return it != null && it.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // If iterator doesn't exist (null), throw exception
        if (it == null) {
            throw new NoSuchElementException("Iterator not open");
        }
        // Otherwise, return the next tuple in the iterator
        return it.next();
    }

    public void close() {
        // If iterator it is started (not null) then close
        if (it != null) {
            it.close();
        }
        // Set iterator it to null
        it = null;
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // Reset state by closing
        close();
        // Restart by opening again from the top
        open();
    }
}
