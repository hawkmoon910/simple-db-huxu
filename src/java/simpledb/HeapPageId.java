package simpledb;

/** Unique identifier for HeapPage objects. */
public class HeapPageId implements PageId {

    // Id of table on this page
    private final int tableId;

    // Page number of this page in the table
    private final int pgNo;

    /**
     * Constructor. Create a page id structure for a specific page of a
     * specific table.
     *
     * @param tableId The table that is being referenced
     * @param pgNo The page number in that table.
     */
    public HeapPageId(int tableId, int pgNo) {
        // Sets tableId and pgNo to given ones
        this.tableId = tableId;
        this.pgNo = pgNo;
    }

    /** @return the table associated with this PageId */
    public int getTableId() {
        // Returns tableId
        return this.tableId;
    }

    /**
     * @return the page number in the table getTableId() associated with
     *   this PageId
     */
    public int getPageNumber() {
        // Returns page number
        return this.pgNo;
    }

    /**
     * @return a hash code for this page, represented by the concatenation of
     *   the table number and the page number (needed if a PageId is used as a
     *   key in a hash table in the BufferPool, for example.)
     * @see BufferPool
     */
    public int hashCode() {
        // Generates hashcode by multiplying tableId with 31 then adding pgNo
        return 31 * tableId + pgNo;
    }

    /**
     * Compares one PageId to another.
     *
     * @param o The object to compare against (must be a PageId)
     * @return true if the objects are equal (e.g., page numbers and table
     *   ids are the same)
     */
    public boolean equals(Object o) {
        // Checks for equality, returns true if equal
        if (this == o) {
            return true;
        }

        // Checks if null or type mismatch, returns false
        if (o == null || !(o instanceof HeapPageId)) {
            return false;
        }

        // Typecasts Object o to a HeapPageId
        HeapPageId other = (HeapPageId) o;
        
        // Returns true if tableIds and pgNos are equal to object o ones
        return this.tableId == other.tableId && this.pgNo == other.pgNo;
    }

    /**
     *  Return a representation of this object as an array of
     *  integers, for writing to disk.  Size of returned array must contain
     *  number of integers that corresponds to number of args to one of the
     *  constructors.
     */
    public int[] serialize() {
        int data[] = new int[2];

        data[0] = getTableId();
        data[1] = getPageNumber();

        return data;
    }

}
