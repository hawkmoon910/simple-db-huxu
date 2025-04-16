package simpledb;

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

    // The file for the heap file on disk
    private final File file;
    
    // Description of the schema
    private final TupleDesc tupleDesc;
    
    // Table Id for heap file
    private final int tableId;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // Set file to f
        this.file = f;
        // Set schema description to td
        this.tupleDesc = td;
        // Use file path hash as unique ID
        this.tableId = f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // Returns the file
        return this.file;
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
        // Returns ID for this heapfile
        return this.tableId;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // Returns description of schema in this file
        return this.tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // If pid is invalid, throw exception
        if (!(pid instanceof HeapPageId)) {
            throw new IllegalArgumentException("Invalid PageId type.");
        }

        // Set hpid to pid typecasted to HeapPageId type
        HeapPageId hpid = (HeapPageId) pid;
        // Set pageSize to page size of buffer pool
        int pageSize = BufferPool.getPageSize();
        // Create new byte array of page size
        byte[] pageData = new byte[pageSize];
        // Set offset to page number * page size
        int offset = hpid.getPageNumber() * pageSize;

        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            // Check if the offset is within bounds
            if (offset + pageSize > raf.length()) {
                throw new IllegalArgumentException("Page offset out of bounds.");
            }

            // Read the page from disk into a byte array
            raf.seek(offset);
            raf.readFully(pageData);

            // Return a HeapPage using the data
            return new HeapPage(hpid, pageData);
        } catch (IOException e) {
            // Throw exception if page can't be read
            throw new IllegalArgumentException("Could not read page", e);
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // Return number of pages which is length of file divided by size of cache
        return (int) (file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // Returns a new Dbfile iterator
        return new DbFileIterator() {
            // Current page number in iteration
            private int currentPage = 0;
            // Iterator for tuples on current page
            private Iterator<Tuple> tupleIter = null;
            // If iterator is open or not
            private boolean open = false;
            
            /**
             * Helper method to get an iterator over tuples on the page.
             */
            private Iterator<Tuple> getTupleIterator(int pageNo) throws DbException, TransactionAbortedException {
                // If the page number is greater than number of pages, return an empty iterator
                if (pageNo >= numPages()) {
                    return Collections.emptyIterator();
                }

                // Get the page from the buffer pool with READ_ONLY permission
                HeapPageId pid = new HeapPageId(getId(), pageNo);
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                
                // Return the iterator over tuples for the page
                return page.iterator();
            }

            @Override
            public void open() throws DbException, TransactionAbortedException {
                // Sets open to true
                open = true;
                // Sets current page to zero
                currentPage = 0;
                // Start with first page
                tupleIter = getTupleIterator(currentPage);
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                // If iterator closed, return false
                if (!open) {
                    return false;
                }

                // Continue to the next page until a tuple is found or all pages are exhausted
                while ((tupleIter == null || !tupleIter.hasNext()) && currentPage < numPages() - 1) {
                    currentPage++;
                    tupleIter = getTupleIterator(currentPage);
                }

                return tupleIter != null && tupleIter.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException {
                // If there isn't a next, throw exception
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                // Return the next tuple
                return tupleIter.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                // Restart iterator by closing and reopening
                close();
                open();
            }

            @Override
            public void close() {
                // Closes iterator
                open = false;
                // Empties the iterator
                tupleIter = null;
            }
        };
    }

}

