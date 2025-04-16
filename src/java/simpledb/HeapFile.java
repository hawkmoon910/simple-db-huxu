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

    private final File file;
    
    private final TupleDesc tupleDesc;
    
    private final int tableId;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.tupleDesc = td;
        this.tableId = f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
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
        return this.tableId;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        if (!(pid instanceof HeapPageId)) {
            throw new IllegalArgumentException("Invalid PageId type.");
        }

        HeapPageId hpid = (HeapPageId) pid;
        int pageSize = BufferPool.getPageSize();
        byte[] pageData = new byte[pageSize];
        int offset = hpid.getPageNumber() * pageSize;

        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            if (offset + pageSize > raf.length()) {
                throw new IllegalArgumentException("Page offset out of bounds.");
            }
            raf.seek(offset);
            raf.readFully(pageData);
            return new HeapPage(hpid, pageData);
        } catch (IOException e) {
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
        return new DbFileIterator() {
            private int currentPage = 0;
            private Iterator<Tuple> tupleIter = null;
            private boolean open = false;

            private Iterator<Tuple> getTupleIterator(int pageNo) throws DbException, TransactionAbortedException {
                if (pageNo >= numPages()) {
                    return Collections.emptyIterator();
                }
                HeapPageId pid = new HeapPageId(getId(), pageNo);
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                return page.iterator();
            }

            @Override
            public void open() throws DbException, TransactionAbortedException {
                open = true;
                currentPage = 0;
                tupleIter = getTupleIterator(currentPage);
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (!open) {
                    return false;
                }

                while ((tupleIter == null || !tupleIter.hasNext()) && currentPage < numPages() - 1) {
                    currentPage++;
                    tupleIter = getTupleIterator(currentPage);
                }

                return tupleIter != null && tupleIter.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }

                return tupleIter.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                close();
                open();
            }

            @Override
            public void close() {
                open = false;
                tupleIter = null;
            }
        };
    }

}

