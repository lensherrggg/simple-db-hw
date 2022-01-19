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

    private DbFile file;
    private int ioCostPerPage;
    private int nTup;
    private TupleDesc td;
    private ConcurrentHashMap<Integer, IntHistogram> intHistogramMap;
    private ConcurrentHashMap<Integer, StringHistogram> stringHistogramMap;

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
        // Done
        file = Database.getCatalog().getDatabaseFile(tableid);
        this.ioCostPerPage = ioCostPerPage;
        nTup = 0;

        // generate a histogram for all fields
        intHistogramMap = new ConcurrentHashMap<>();
        stringHistogramMap = new ConcurrentHashMap<>();

        // get some metadata
        td = file.getTupleDesc();
        int numFields = td.numFields();
        int[] minArr = new int[numFields];
        int[] maxArr = new int[numFields];
        for (int i = 0; i < numFields; i++) {
            minArr[i] = Integer.MAX_VALUE;
            maxArr[i] = Integer.MIN_VALUE;
        }

        SeqScan scan = new SeqScan(new TransactionId(), tableid);
        try {
            scan.open();
            // scan the whole table to find the min and max of each field
            while (scan.hasNext()) {
                Tuple t = scan.next();
                nTup++;
                for (int i = 0; i < numFields; i++) {
                    int fieldVal = t.getField(i).hashCode();
                    minArr[i] = Math.min(minArr[i], fieldVal);
                    maxArr[i] = Math.max(maxArr[i], fieldVal);
                }
            }

            // initialize histograms
            for (int i = 0; i < numFields; i++) {
                Type fieldType = td.getFieldType(i);
                switch (fieldType) {
                    case INT_TYPE:
                        intHistogramMap.put(i, new IntHistogram(NUM_HIST_BINS, minArr[i], maxArr[i]));
                        break;
                    case STRING_TYPE:
                        stringHistogramMap.put(i, new StringHistogram(NUM_HIST_BINS));
                        break;
                    default:
                        throw new UnsupportedOperationException("Unsupported field type");
                }
            }

            scan.rewind();
            // scan the whole table to populate the counts of buckets in each histogram
            while (scan.hasNext()) {
                Tuple t = scan.next();
                for (int i = 0; i < numFields; i++) {
                    Type fieldType = td.getFieldType(i);
                    switch (fieldType) {
                        case INT_TYPE:
                            IntField intField = (IntField) t.getField(i);
                            intHistogramMap.get(i).addValue(intField.getValue());
                            break;
                        case STRING_TYPE:
                            StringField stringField = (StringField) t.getField(i);
                            stringHistogramMap.get(i).addValue(stringField.getValue());
                            break;
                        default:
                            throw new UnsupportedOperationException("Unsupported field type");
                    }
                }
            }

        } catch (DbException | TransactionAbortedException e) {
            e.printStackTrace();
        } finally {
            scan.close();
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
        // Done
        if (file instanceof HeapFile) {
            HeapFile f = (HeapFile) file;
            return f.numPages() * ioCostPerPage;
        }
        throw new UnsupportedOperationException("Unsupported file type");
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
        // Done
        return (int) (totalTuples() * selectivityFactor);
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
        // Done
        switch (td.getFieldType(field)) {
            case INT_TYPE:
                return intHistogramMap.get(field).avgSelectivity();
            case STRING_TYPE:
                return stringHistogramMap.get(field).avgSelectivity();
            default:
                throw new UnsupportedOperationException("Unsupported field type");
        }
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
        // Done
        switch (td.getFieldType(field)) {
            case INT_TYPE:
                IntField intField = (IntField) constant;
                return intHistogramMap.get(field).estimateSelectivity(op, intField.getValue());
            case STRING_TYPE:
                StringField stringField = (StringField) constant;
                return stringHistogramMap.get(field).estimateSelectivity(op, stringField.getValue());
            default:
                throw new UnsupportedOperationException("Unsupported field type");
        }
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // Done
        return nTup;
    }

}
