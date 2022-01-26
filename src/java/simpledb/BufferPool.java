package simpledb;

import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private int numPages;
    private ConcurrentHashMap<Integer, Page> buffer;
    private LockManager lockManager;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // Done
        this.numPages = numPages;
        buffer = new ConcurrentHashMap<>();
        lockManager = new LockManager();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        LockType lockType = null;
        if (perm == Permissions.READ_ONLY) {
            lockType = LockType.SHARED_LOCK;
        } else {
            lockType = LockType.EXCLUSIVE_LOCK;
        }

        long st = System.currentTimeMillis();
        while (true) {
            if (lockManager.acquireLock(pid, tid, lockType)) {
                break;
            }
            // timeout interruption
            // if a transaction fails to acquire a lock within limited time, we assume that deadlock exists
            // abort the transaction
            long now = System.currentTimeMillis();
            if (now - st > 100) {
                throw new TransactionAbortedException();
            }
        }

        int key = pid.hashCode();
        if (!buffer.containsKey(key)) {
            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            while (buffer.size() >= numPages) {
                evictPage();
            }
            Page page = dbFile.readPage(pid);
            buffer.put(key, page);
            return page;
        }

        return buffer.get(key);
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // Done
        lockManager.releaseLock(pid, tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // Done
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // Done
        return lockManager.holdsLock(p, tid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // Done
        // if commit flush all pages, else recover
        if (commit) {
//            flushPages(tid);
            for (Page page : buffer.values()) {
                TransactionId pageTid = page.isDirty();
                if (null != pageTid && pageTid == tid) {
                    flushPage(page.getId());
                    page.setBeforeImage();
                }
                if (null == pageTid) {
                    page.setBeforeImage();
                }
            }
        } else {
            restorePages(tid);
        }
        // release lock
        lockManager.releaseLock(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // Done
        ArrayList<Page> pages = Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t);
        updateBufferPool(tid, pages);
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // Done
        ArrayList<Page> pages = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId()).deleteTuple(tid, t);
        updateBufferPool(tid, pages);
    }

    /**
     * Mark page dirty and put it into buffer pool
     * @param tid the transaction that updates pages
     * @param pages updated pages
     */
    public void updateBufferPool(TransactionId tid, List<Page> pages) {
        for (Page p : pages) {
            // mark dirty
            p.markDirty(true, tid);
            // evict pages
            int key = p.getId().hashCode();
            if (!this.buffer.containsKey(key)) {
                while (this.buffer.size() >= numPages) {
                    try {
                        evictPage();
                    } catch (DbException e) {
                        e.printStackTrace();
                    }
                }
            }
            this.buffer.put(key, p);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // Done
        for (Page p : buffer.values()) {
            flushPage(p.getId());
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // Done
        buffer.remove(pid.hashCode());
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // Done
        Page p = buffer.get(pid.hashCode());
        if (null != p.isDirty()) {
            DbFile dbFile = Database.getCatalog().getDatabaseFile(p.getId().getTableId());
            TransactionId dirtier = p.isDirty();
            Database.getLogFile().logWrite(dirtier, p.getBeforeImage(), p);
            Database.getLogFile().force();
            dbFile.writePage(p);
            p.markDirty(false, null);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // Done
        for (Page page : buffer.values()) {
            TransactionId pageTid = page.isDirty();
            if (null != pageTid && pageTid == tid) {
                flushPage(page.getId());
                page.setBeforeImage();
            }
        }
    }

    /** Restore all pages of a specified transaction from disk
     */
    public synchronized void restorePages(TransactionId tid) throws IOException {
        for (Page page : buffer.values()) {
            TransactionId pageTid = page.isDirty();
            if (null != pageTid && pageTid == tid) {
                // discard the dirty page and retrieve from disk again when needed
                discardPage(page.getId());
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // Done
        // random eviction
        List<Integer> keys = new ArrayList<>(buffer.keySet());
        Page pageToEvict = null;
        Set<PageId> dirtyPageIds = new HashSet<>();
        boolean foundDirtyPage = false;
        while (!foundDirtyPage) {
            int randomKey = keys.get(new Random().nextInt(keys.size()));
            pageToEvict = buffer.get(randomKey);
            if (null != pageToEvict.isDirty()) {
                // dirty page
                dirtyPageIds.add(pageToEvict.getId());
                if (dirtyPageIds.size() > numPages) {
                    break;
                }
                continue;
            }
            foundDirtyPage = true;
        }

        if (!foundDirtyPage) {
            // no clean pages
            throw new DbException("All pages in buffer pool is dirty");
        }

        PageId evictPid = pageToEvict.getId();
        try {
            flushPage(evictPid);
        } catch (IOException e) {
            e.printStackTrace();
        }
        discardPage(evictPid);
    }

    public enum LockType {
        SHARED_LOCK, EXCLUSIVE_LOCK;
    }

    private class Lock {
        TransactionId tid;
        LockType lockType;

        public Lock(TransactionId tid, LockType lockType) {
            this.tid = tid;
            this.lockType = lockType;
        }
    }

    private class LockManager {
        private ConcurrentHashMap<PageId, Vector<Lock>> pageLocks;

        public LockManager() {
            pageLocks = new ConcurrentHashMap<>();
        }

        /**
         * Try to acquire a given lock
         * @param pageId the id of page that requires a lock
         * @param tid the id of transaction that requires a lock
         * @param lockType the type of lock to be acquired
         * @return if the acquisition of lock succeeded
         */
        public synchronized boolean acquireLock(PageId pageId, TransactionId tid, LockType lockType) {
            Vector<Lock> locks = pageLocks.get(pageId);
            if (null == locks) {
                Lock lock = new Lock(tid, lockType);
                locks = new Vector<>();
                locks.add(lock);
                pageLocks.put(pageId, locks);
                return true;
            }

            for (Lock lock : locks) {
                if (lock.tid == tid) {
                    if (lock.lockType == lockType) {
                        // already holds that kind of lock
                        return true;
                    }
                    if (lock.lockType == LockType.EXCLUSIVE_LOCK) {
                        // already holds an exclusive lock
                        return true;
                    }
                    if (locks.size() == 1) {
                        // already holds one shared lock, upgrade it to exclusive lock
                        lock.lockType = LockType.EXCLUSIVE_LOCK;
                        return true;
                    }
                    return false;
                }
            }

            if (locks.get(0).lockType == LockType.EXCLUSIVE_LOCK) {
                // the page already holds an exclusive lock
                return false;
            }

            if (lockType == LockType.SHARED_LOCK) {
                // acquire a new shared lock
                Lock lock = new Lock(tid, LockType.SHARED_LOCK);
                locks.add(lock);
                pageLocks.put(pageId, locks);
                return true;
            }
            // shared locks exist, could not acquire an exclusive lock
            return false;
        }

        /**
         * Release the lock for a transaction
         * @param tid the transaction whose lock needs to be released
         * @return if lock is successfully released
         */
        public synchronized boolean releaseLock(PageId pageId, TransactionId tid) {
            Vector<Lock> locks = pageLocks.get(pageId);
            assert null != locks: "Page is not locked";
            for (Lock lock : locks) {
                if (lock.tid == tid) {
                    locks.remove(lock);

                    if (locks.size() == 0) {
                        pageLocks.remove(pageId);
                    }

                    return true;
                }
            }
            // could not find tid that locks on pageId
            return false;
        }

        public synchronized void releaseLock(TransactionId tid) {
            for (PageId pid : pageLocks.keySet()) {
                releaseLock(pid, tid);
            }
        }

        /**
         * Determine if a transaction holds a lock
         * @param pageId the id of the page to be checked
         * @param tid the id of the transaction to be checked
         * @return if a transaction holds a lock
         */
        public synchronized boolean holdsLock(PageId pageId, TransactionId tid) {
            Vector<Lock> locks = pageLocks.get(pageId);
            if (null == locks) {
                return false;
            }
            for (Lock lock : locks) {
                if (lock.tid == tid) {
                    return true;
                }
            }
            return false;
        }
    }
}
