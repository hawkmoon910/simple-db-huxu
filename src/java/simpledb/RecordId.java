package simpledb;

import java.io.Serializable;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;

    // Page id of page where tuple is
    private final PageId pid;

    // Tuple number of the page
    private final int tupleno;

    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     * 
     * @param pid
     *            the pageid of the page on which the tuple resides
     * @param tupleno
     *            the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleno) {
        // Sets pid and tupleno to new ones
        this.pid = pid;
        this.tupleno = tupleno;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int getTupleNumber() {
        // Returns tupleno (tuple number)
        return this.tupleno;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        // Returns pid (page id)
        return this.pid;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     * 
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
        // Checks for equality, returns true if equal
        if (this == o) {
            return true;
        }

        // Checks if null or type mismatch, returns false
        if (o == null || !(o instanceof RecordId)) {
            return false;
        }
        
        // Typecasts Object o to a RecordId
        RecordId other = (RecordId) o;

        // Returns true if tableIds and pgNos are equal to object o ones
        return this.tupleno == other.tupleno && this.pid.equals(other.pid);
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * 
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
        // Returns hashcode which is 31 times hashcode of pid plus tupleno
        return 31 * pid.hashCode() + tupleno;
    }

}
