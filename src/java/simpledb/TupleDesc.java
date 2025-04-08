package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        return items.iterator(); // Return iterator over field list
    }

    private static final long serialVersionUID = 1L;
    
    // List of fields (TDItem)
    private List<TDItem> items;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // Initialize the list of TDItems
        items = new ArrayList<>();
        // For loop creates a TDItem for each field with optional name until length of typeAr, null if typeAr is too short
        for (int i = 0; i < typeAr.length; i++) {
            items.add(new TDItem(typeAr[i], (fieldAr != null && i < fieldAr.length) ? fieldAr[i] : null));
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // Calls constructor above with null fields
        this(typeAr, new String[typeAr.length]);
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return items.size(); // Returns size of list
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // Invalid index check, throw NoSuchElementException
        if (i < 0 || i >= items.size()) {
            throw new NoSuchElementException();
        }
        // Returns field name at index i of list items
        return items.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // Invalid index check, throw NoSuchElementException
        if (i < 0 || i >= items.size()) {
            throw new NoSuchElementException();
        }
        // Returns field type at index i of list items
        return items.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        for (int i = 0; i < items.size(); i++) {
            // If fieldName isn't empty/null and the name matches with fieldName return index where it is found
            if (items.get(i).fieldName != null && items.get(i).fieldName.equals(name)) {
                return i;
            }
        }
        throw new NoSuchElementException("Field name not found: " + name); // Not found, throw NoSuchElementException
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // Total size
        int size = 0;
        // Add length/size of item for all items
        for (TDItem item : items) {
            size += item.fieldType.getLen();
        }
        // Return total size
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        int totalFields = td1.numFields() + td2.numFields(); // Total fields
        Type[] types = new Type[totalFields]; // Array for merged types
        String[] names = new String[totalFields]; // Array for merged names

        // Copys td1 fields
        for (int i = 0; i < td1.numFields(); i++) {
            types[i] = td1.getFieldType(i);
            names[i] = td1.getFieldName(i);
        }

        // Copy td2 fields
        for (int i = 0; i < td2.numFields(); i++) {
            types[i + td1.numFields()] = td2.getFieldType(i);
            names[i + td1.numFields()] = td2.getFieldName(i);
        }

        // Returns new TupleDesc
        return new TupleDesc(types, names);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // Same object, returns true
        if (this == o) {
            return true;
        }

        // Not a TupleDesc, returns false
        if (!(o instanceof TupleDesc)) {
            return false;
        }
        
        TupleDesc other = (TupleDesc) o;
        // Number of fields aren't the same, return false
        if (this.numFields() != other.numFields()) {
            return false;
        }

        // For loop that checks for field type mismatch, returns false
        for (int i = 0; i < this.numFields(); i++) {
            if (!this.getFieldType(i).equals(other.getFieldType(i))) {
                return false;
            }
        }

        // Passed all checks, returns true
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        return Objects.hash(items); // Returns hash based on items list
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        StringBuilder sb = new StringBuilder();
        // For each field (TDItem) in the schema, append a description to the string
        for (TDItem item : items) {
            sb.append(item.fieldType).append("(").append(item.fieldName).append(")");
            // Only add a comma if this is not the last field
            if (i < items.size() - 1) {
                sb.append(", ");
            }
        }
        // Return StringBuilder sb as a string
        return sb.toString();
    }
}
