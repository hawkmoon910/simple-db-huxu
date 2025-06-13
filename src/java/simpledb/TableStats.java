package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;
    // Cost per page
    private final int ioCostPerPage;
    // Reference to physical storage file
    private final HeapFile dbFile;
    // Schema of the table
    private final TupleDesc tupleDesc;
    // Creates map for histograms for int fields
    private final Map<Integer, IntHistogram> intHistograms;
    // Creates map for histograms for string fields
    private final Map<Integer, StringHistogram> stringHistograms;
    // Count of the total tuples in the table
    private int totalTuples;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.

        // Store the IO cost per page
        this.ioCostPerPage = ioCostPerPage;
        // Gets the heap file representing the table from the catalog
        this.dbFile = (HeapFile) Database.getCatalog().getDatabaseFile(tableid);
        // Get schema of the table
        this.tupleDesc = dbFile.getTupleDesc();
        // Initialize map to store int histogram
        this.intHistograms = new HashMap<>();
        // Initialize map to store string histogram
        this.stringHistograms = new HashMap<>();
        // Initialize count for tuples to zero
        this.totalTuples = 0;
        // Create an iterator
        DbFileIterator it = dbFile.iterator(new TransactionId());
        // Temporary map to track min values for each integer field
        Map<Integer, Integer> minMap = new HashMap<>();
        // Temporary map to track max values for each integer field
        Map<Integer, Integer> maxMap = new HashMap<>();

        // Determine min and max for each INT field, and count total tuples
        try {
            // Open the iterator
            it.open();
            // Iterate through all tuples
            while (it.hasNext()) {
                // Get the next tuple
                Tuple tuple = it.next();
                // Increment total tuple count
                totalTuples++;
                // Loop through each field in the tuple
                for (int i = 0; i < tupleDesc.numFields(); i++) {
                    // Get field type
                    Type type = tupleDesc.getFieldType(i);
                    // Check if type is int
                    if (type == Type.INT_TYPE) {
                        // Get integer value
                        int val = ((IntField) tuple.getField(i)).getValue();
                        // Update min map for the field
                        minMap.put(i, Math.min(minMap.getOrDefault(i, val), val));
                        // Update max map for the field
                        maxMap.put(i, Math.max(maxMap.getOrDefault(i, val), val));
                    }
                }
            }
            // Close the iterator
            it.close();
        } catch (Exception e) {
            // Print any exceptions encountered
            e.printStackTrace();
        }
        // Create IntHistograms using collected min/max ranges for int fields
        for (Map.Entry<Integer, Integer> entry : minMap.entrySet()) {
            // Get field index
            int fieldIndex = entry.getKey();
            // Get min value
            int min = minMap.get(fieldIndex);
            // Get max value
            int max = maxMap.get(fieldIndex);
            // Create histogram with specified number of bins and min/max
            intHistograms.put(fieldIndex, new IntHistogram(NUM_HIST_BINS, min, max));
        }
        // Initialize empty StringHistograms for all string fields
        for (int i = 0; i < tupleDesc.numFields(); i++) {
            // Check if schema is a string
            if (tupleDesc.getFieldType(i) == Type.STRING_TYPE) {
                // Create histogram with number of bins (100)
                stringHistograms.put(i, new StringHistogram(NUM_HIST_BINS));
            }
        }

        // Populate the histograms with actual values
        try {
            // Re-open the iterator
            it.open();
            // Loop through all tuples again
            while (it.hasNext()) {
                // Get next tuple
                Tuple tuple = it.next();
                // Loop through each field in the tuple
                for (int i = 0; i < tupleDesc.numFields(); i++) {
                    // Get the field
                    Field field = tuple.getField(i);
                    // Checks if type is int
                    if (field.getType() == Type.INT_TYPE) {
                        // Add int value to int histogram
                        intHistograms.get(i).addValue(((IntField) field).getValue());
                    // Checks if type is string
                    } else if (field.getType() == Type.STRING_TYPE) {
                        // Add string value to string histogram
                        stringHistograms.get(i).addValue(((StringField) field).getValue());
                    }
                }
            }
            // Close the iterator
            it.close();
        } catch (Exception e) {
            // Print any exceptions encountered
            e.printStackTrace();
        }
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // Estimate scan cost as pages * cost per page
        return dbFile.numPages() * (double) ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // Estimate output cardinality
        return (int) Math.round(totalTuples * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // Checks if map of int fields includes this field
        if (intHistograms.containsKey(field)) {
            // Average for int
            return intHistograms.get(field).avgSelectivity();
        // Checks if map of string fields includes this field
        } else if (stringHistograms.containsKey(field)) {
            // Average for string
            return stringHistograms.get(field).avgSelectivity();
        }
        // Default
        return 1.0; 
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // Checks if type is int and if map of int fields includes this field
        if (constant.getType() == Type.INT_TYPE && intHistograms.containsKey(field)) {
            // Get the estimated selectivity for int fields
            return intHistograms.get(field).estimateSelectivity(op, ((IntField) constant).getValue());
        // Checks if type is string and if map of string fields includes this field
        } else if (constant.getType() == Type.STRING_TYPE && stringHistograms.containsKey(field)) {
            // Get the estimated selectivity for string fields
            return stringHistograms.get(field).estimateSelectivity(op, ((StringField) constant).getValue());
        }
        return 1.0;
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // Return total tuples
        return totalTuples;
    }

}
