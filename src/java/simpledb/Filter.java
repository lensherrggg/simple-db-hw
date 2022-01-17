package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    private Predicate p;
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
        // Done
        this.p = p;
        this.child = child;
    }

    public Predicate getPredicate() {
        // Done
        return p;
    }

    public TupleDesc getTupleDesc() {
        // Done
        return child.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // Done
        child.open();
        super.open();
    }

    public void close() {
        // Done
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // Done
        child.rewind();
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
        // Done
        while (child.hasNext()) {
            Tuple t = child.next();
            if (p.filter(t)) {
                return t;
            }
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // Done
        return new OpIterator[] {child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // Done
        if (children[0] != child) {
            children[0] = child;
        }
    }

}
