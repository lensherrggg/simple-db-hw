package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private static TupleDesc RES_TUPLE_DESC = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{""});

    private TransactionId tid;
    private OpIterator child;
    private boolean called;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // Done
        tid = t;
        this.child = child;
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
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
                Database.getBufferPool().deleteTuple(tid, t);
            } catch (IOException e) {
                e.printStackTrace();
            }
            count++;
        }
        called = true;
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
        child = children[0];
    }

}
