package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator child;
    private int aField;
    private int gField;
    private Aggregator.Op aop;
    private Aggregator aggregator;
    private OpIterator resultIterator;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The OpIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
	    // Done
        this.child = child;
        this.aField = afield;
        this.gField = gfield;
        this.aop = aop;
        Type gbType = gfield == Aggregator.NO_GROUPING ? null : child.getTupleDesc().getFieldType(gfield);
        switch (child.getTupleDesc().getFieldType(afield)) {
            case INT_TYPE: aggregator = new IntegerAggregator(gfield, gbType, afield, aop); break;
            case STRING_TYPE: aggregator = new StringAggregator(gfield, gbType, afield, aop); break;
            default:
                throw new IllegalArgumentException("Unknown group by field type");
        }

        try {
            child.open();
            while (child.hasNext()) {
                aggregator.mergeTupleIntoGroup(child.next());
            }
            child.close();
        } catch (DbException | TransactionAbortedException e) {
            e.printStackTrace();
        }

        resultIterator = null;
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
        // Done
        return gField;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     * */
    public String groupFieldName() {
        // Done
        return child.getTupleDesc().getFieldName(gField);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
        // Done
        return aField;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
        // Done
        return child.getTupleDesc().getFieldName(aField);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
        // Done
        return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
	    // Done
        resultIterator = aggregator.iterator();
        resultIterator.open();
        super.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // Done
        if (null != resultIterator && resultIterator.hasNext()) {
            return resultIterator.next();
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
	    // Done
        if (null != resultIterator) {
            resultIterator.rewind();
        }
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        // Done
        return child.getTupleDesc();
    }

    public void close() {
	    // Done
        super.close();
        resultIterator = null;
    }

    @Override
    public OpIterator[] getChildren() {
        // Done
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
	    // Done
        try {
            if (child != children[0]) {
                child = children[0];
                child.open();
                while (child.hasNext()) {
                    aggregator.mergeTupleIntoGroup(child.next());
                }
                child.close();
            }
        } catch (DbException | TransactionAbortedException e) {
            e.printStackTrace();
        }
    }
    
}
