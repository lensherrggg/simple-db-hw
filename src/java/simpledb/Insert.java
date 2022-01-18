package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private static TupleDesc RES_TUPLE_DESC = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{""});

    private TransactionId tid;
    private OpIterator child;
    private int tableId;
    /* check if is called more than once */
    private boolean called;
    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // Done
        tid = t;
        this.child = child;
        this.tableId = tableId;
        called = false;
    }

    public TupleDesc getTupleDesc() {
        // Done
        return RES_TUPLE_DESC;
    }

    public void open() throws DbException, TransactionAbortedException {
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
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // Done
        if (called) {
            return null;
        }
        int count = 0;
        while (child.hasNext()) {
            Tuple t = child.next();
            try {
                Database.getBufferPool().insertTuple(tid, tableId, t);
            } catch (IOException e) {
                e.printStackTrace();
            }
            count++;
        }
        Tuple res = new Tuple(RES_TUPLE_DESC);
        res.setField(0, new IntField(count));
        return res;
    }

    @Override
    public OpIterator[] getChildren() {
        // Done
        return new OpIterator[] {child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // Done
        this.child = children[0];
    }
}
