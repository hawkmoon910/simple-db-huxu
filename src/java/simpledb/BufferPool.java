package simpledb;

import java.io.*;

import java.util.concurrent.ConcurrentHashMap;
import java.util.*;

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

    // Max pages buffer pool can hold
    private final int numPages;

    // ConcurrentHashMap used for thread-safe page cache
    private final ConcurrentHashMap<PageId, Page> pageCache;

    // LockManager to manage locks
    private final LockManager lockManager = new LockManager();

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // Sets numPages to current numPages
        this.numPages = numPages;
        // Creates a new conccurent hashmap
        this.pageCache = new ConcurrentHashMap<>();
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
        
        // Acquires lock
        lockManager.acquireLock(tid, pid, perm);
        
        // Try fetching the page from the cache
        Page page = pageCache.get(pid);

        // If page is in the cache, return page
        if (page != null) {
            return page;
        }

        // If the buffer pool (cache) is full, evict page
        if (pageCache.size() >= numPages) {
            evictPage();
        }

        // Get the Dbfile from the table of this page
        DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());

        // Read the page
        page = file.readPage(pid);

        // Add the page to the cache (buffer pool)
        pageCache.put(pid, page);

        // Return the page
        return page;
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
        // Releases the lcok
        lockManager.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // Calls transactionComplete with true for commit, so it actually releases all locks
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // Checks if lockManager has a lock
        return lockManager.holdsLock(tid, p);
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
        // List to keep track of all pages modified by this transaction
        List<PageId> pagesToProcess = new ArrayList<>();
        // Iterate over all pages in the buffer pool
        for (Map.Entry<PageId, Page> entry : pageCache.entrySet()) {
            // Get page
            Page page = entry.getValue();
            // Check if the page was dirtied by this transaction
            if (tid.equals(page.isDirty())) {
                // Add page id to list
                pagesToProcess.add(entry.getKey());
            }
        }
        // Process all dirty pages associated with this transaction
        for (PageId pid : pagesToProcess) {
            Page page = pageCache.get(pid);
            if (page == null) {
                continue;
            }
            if (commit) {
                // Log
                Database.getLogFile().logWrite(tid, page.getBeforeImage(), page);
                // Rollback point
                page.setBeforeImage();
                // Ensure log record is persisted
                Database.getLogFile().force();
                // Mark as clean if needed
                page.markDirty(false, null);
            } else {
                // Abort
                // Get the DbFile corresponding to the page
                DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
                // Read the clean version of the page from disk
                Page cleanPage = file.readPage(pid);
                // Replace the dirty page in the buffer pool with the clean one
                pageCache.put(pid, cleanPage);
            }
        }

        // Release all locks regardless of commit/abort
        lockManager.releaseAllLocks(tid);
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
        // Get the DbFile connected to the table ID
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        // Insert the tuple into the table, which returns any pages that were modified
        ArrayList<Page> dirtyPages = file.insertTuple(tid, t);
        // For each dirty page returned
        for (Page p : dirtyPages) {
            // Mark the page as dirty
            p.markDirty(true, tid);
            // Evict if buffer pool is full
            if (pageCache.size() >= numPages && !pageCache.containsKey(p.getId())) {
                evictPage();
            }   
            // Put the page into the buffer pool (cache), replace any existing version
            pageCache.put(p.getId(), p);
        }
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
        // Get the table ID of the page containing the tuple
        int tableId = t.getRecordId().getPageId().getTableId();
        // Get the DbFile connected to that table
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        // Delete the tuple from the file, returning any pages that were modified
        ArrayList<Page> dirtyPages = file.deleteTuple(tid, t);
        // For each dirty page returned
        for (Page p : dirtyPages) {
            // Mark the page as dirty
            p.markDirty(true, tid);
            // Evict if buffer pool is full
            if (pageCache.size() >= numPages && !pageCache.containsKey(p.getId())) {
                evictPage();
            }   
            // Put the page into the buffer pool (cache), replace any existing version
            pageCache.put(p.getId(), p);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // Iterate through all pages currently in the buffer pool
        for (PageId pid : pageCache.keySet()) {
            // Flush each page
            Page p = pageCache.get(pid);
            if (p != null) {
                flushPage(pid); // consider removing the dirty check in flushPage
            }
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
        // Remove page from buffer pool (cache)
        pageCache.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // Get the page from the buffer pool
        Page p = pageCache.get(pid);
        // If the page doesn't exist or isn't dirty, return without flushing
        if (p == null || p.isDirty() == null) {
            return;
        }
        TransactionId dirtier = p.isDirty();
        // Only log if the transaction is still active
        if (dirtier != null && Database.getBufferPool().holdsLock(dirtier, pid)) {
            Database.getLogFile().logWrite(dirtier, p.getBeforeImage(), p);
            Database.getLogFile().force();
        }
        // Write to disk
        Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(p);
        // After writing, mark the page as clean
        p.markDirty(false, null);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // Iterate through all pages in the buffer pool (cache)
        for (Map.Entry<PageId, Page> entry : pageCache.entrySet()) {
            // Get the page id
            PageId pid = entry.getKey();
            // Get the page
            Page page = entry.getValue();

            try {
                // Flush the clean page to disk
                flushPage(pid);
                // Discard the page from the buffer pool
                discardPage(pid);
                // Exit
                return;
            } catch (IOException e) {
                // If an IO error occurs, throw exception
                throw new DbException("IO error during eviction: " + e.getMessage());
            }
        }
        // throw exception
        throw new DbException("All pages could not be evicted.");
    }

}
