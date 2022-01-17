package simpledb;

import javax.xml.crypto.Data;
import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File f;
    private TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // Done
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // Done
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // Done
        return f.getAbsolutePath().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // Done
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // Done
        int tid = pid.getTableId();
        int pgNo = pid.getPageNumber();
        RandomAccessFile f = null;
        try {
            f = new RandomAccessFile(this.f, "r");
            int pgSz = BufferPool.getPageSize();
            // page number starts with 0
            if ((long) (pgNo + 1) * pgSz > f.length()) {
                throw new IllegalArgumentException(String.format("table %d page %d is invalid", tid, pgNo));
            }
            byte[] bytes = new byte[pgSz];
            // calculate offset
            f.seek((long) pgNo * pgSz);
            int r = f.read(bytes, 0, pgSz);
            if (r != pgSz) {
                throw new IllegalArgumentException(String.format("table %d page %d is invalid", tid, pgNo));
            }
            return new HeapPage((HeapPageId) pid, bytes);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (f != null) {
                try {
                    f.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        throw new IllegalArgumentException(String.format("table %d page %d is invalid", tid, pgNo));
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // Done
        RandomAccessFile f = new RandomAccessFile(this.f, "rw");
        int pgNo = page.getId().getPageNumber();
        f.seek((long) pgNo * BufferPool.getPageSize());
        f.write(page.getPageData());
        f.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // Done
        return (int) Math.ceil(f.length() * 1.0 / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // Done
        ArrayList<Page> modified = new ArrayList<>();
        for (int i = 0; i < numPages(); i++) {
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), i), Permissions.READ_WRITE);
            if (page.getNumEmptySlots() > 0) {
                page.insertTuple(t);
                modified.add(page);
                break;
            }
        }
        if (modified.isEmpty()) {
            int newPageNumber = numPages();
            RandomAccessFile f = new RandomAccessFile(this.f, "rw");
            f.seek(f.length());
            byte[] data = HeapPage.createEmptyPageData();
            f.write(data);
            f.close();
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), newPageNumber), Permissions.READ_WRITE);
            page.insertTuple(t);
            modified.add(page);
        }
        return modified;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        ArrayList<Page> modified = new ArrayList<>();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);
        modified.add(page);
        return modified;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // Done
        return new HeapFileIterator(this, tid);
    }

    private class HeapFileIterator extends AbstractDbFileIterator {
        private HeapFile file;
        private TransactionId tid;
        private int currentPage;
        private Iterator<Tuple> iterator;

        public HeapFileIterator(HeapFile file, TransactionId tid) {
            this.file = file;
            this.tid = tid;
        }

        public Iterator<Tuple> initializeIterator(int pgNo) throws DbException, TransactionAbortedException {
            if (pgNo < 0 || pgNo >= file.numPages()) {
                throw new DbException(String.format("problems with opening/accessing the database pageNo %d ", pgNo));
            }
            HeapPageId pgId = new HeapPageId(file.getId(), pgNo);
            /*
                copied from https://github.com/jasonleaster/simple-db/blob/master/src/java/simpledb/dbfile/HeapFile.java
                1. 如果使用READ_ONLY, 会导致并发事务的系统单元测试挂掉，无解
                2. 使用READ_WRITE，抢占该页数据，避免并发写覆盖的情况，为了通过单元测试。
             */
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pgId, Permissions.READ_ONLY);
            return page.iterator();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            currentPage = 0;
            iterator = initializeIterator(currentPage);
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (null == iterator) {
                return false;
            } else if (iterator.hasNext()) {
                return true;
            } else {
                currentPage++;
                if (currentPage >= file.numPages()) {
                    return false;
                } else {
                    iterator = initializeIterator(currentPage);
                    return iterator.hasNext();
                }
            }
        }

        @Override
        protected Tuple readNext() throws DbException, TransactionAbortedException {
            if (null == iterator) {
                return null;
            }

            while (!iterator.hasNext()) {
                currentPage++;
                if (currentPage < file.numPages()) {
                    iterator = initializeIterator(currentPage);
                } else {
                    break;
                }
            }

            if (currentPage == file.numPages()) {
                return null;
            }

            return iterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            super.close();
            currentPage = file.numPages();
        }
    }

}

