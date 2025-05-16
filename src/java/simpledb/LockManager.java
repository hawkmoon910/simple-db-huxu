package simpledb;

import java.util.*;

public class LockManager {
    // Enum to represent types of locks: shared (read) or exclusive (write)
    private enum LockType { SHARED, EXCLUSIVE }
    // A Lock consists of the transaction and its type (SHARED or EXCLUSIVE)
    private static class Lock {
        TransactionId tid;
        LockType type;

        Lock(TransactionId tid, LockType type) {
            this.tid = tid;
            this.type = type;
        }
    }

    // Maps page to list of locks held
    private final Map<PageId, List<Lock>> pageLocks = new HashMap<>();
    // Maps transaction to set of pages it holds locks on
    private final Map<TransactionId, Set<PageId>> txnPages = new HashMap<>();
    // Maps transaction to the set of transactions it's waiting for
    private final Map<TransactionId, Set<TransactionId>> waitingGraph = new HashMap<>();

    /**
     * Checks if a transaction currently holds any kind of lock on the specified page.
     */
    public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
        List<Lock> locks = pageLocks.get(pid);
        if (locks == null) return false;
        for (Lock l : locks) {
            if (l.tid.equals(tid)) return true;
        }
        return false;
    }

    /**
     * Releases the lock held by a transaction on a page, and updates threads.
     */
    public synchronized void releaseLock(TransactionId tid, PageId pid) {
        List<Lock> locks = pageLocks.get(pid);
        if (locks != null) {
            // Remove all locks held by this transaction on the page
            locks.removeIf(l -> l.tid.equals(tid));
            // If no locks remain, remove the page entry
            if (locks.isEmpty()) pageLocks.remove(pid);
        }
        // Remove the page from the transaction's set of locked pages
        Set<PageId> pages = txnPages.get(tid);
        if (pages != null) {
            pages.remove(pid);
            if (pages.isEmpty()) txnPages.remove(tid);
        }
        // Remove the transaction from the waits-for graph
        waitingGraph.remove(tid);
        // Remove it from other transactions' waiting sets
        for (Set<TransactionId> waiters : waitingGraph.values()) {
            waiters.remove(tid);
        }
        notifyAll(); // Wake up waiting threads
    }

    /**
     * Releases all locks held by a transaction.
     */
    public synchronized void releaseAllLocks(TransactionId tid) {
        // Get all pages this transaction holds locks on
        Set<PageId> pages = txnPages.getOrDefault(tid, new HashSet<>());
        for (PageId pid : new HashSet<>(pages)) {
            releaseLock(tid, pid); // Release lock on each page
        }
    }

    /**
     * Acquires a lock on a page for a transaction. Blocks if lock can't be granted.
     * Throws TransactionAbortedException if a deadlock is detected.
     */
    public synchronized void acquireLock(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException {
        // Determine desired lock type based on permission
        LockType desired = (perm == Permissions.READ_ONLY) ? LockType.SHARED : LockType.EXCLUSIVE;
        // Wait until the lock can be granted
        while (!canGrant(tid, pid, desired)) {
            // Record who tid is waiting for
            Set<TransactionId> holders = getConflictingHolders(tid, pid, desired);
            waitingGraph.putIfAbsent(tid, new HashSet<>());
            waitingGraph.get(tid).addAll(holders);
            // Deadlock detection
            if (hasCycle(tid, new HashSet<>())) {
                waitingGraph.remove(tid);
                throw new TransactionAbortedException(); // Abort on deadlock
            }
            try {
                wait(); // Wait for the lock to be released
            } catch (InterruptedException e) {
                throw new TransactionAbortedException();
            }
            // Clear waits-for entries after waking up
            waitingGraph.getOrDefault(tid, new HashSet<>()).clear();
        }

        // Grant lock
        List<Lock> locks = pageLocks.computeIfAbsent(pid, k -> new ArrayList<>());

        // Check if this transaction already has a lock it needs to upgrade
        for (Iterator<Lock> it = locks.iterator(); it.hasNext();) {
            Lock l = it.next();
            if (l.tid.equals(tid)) {
                if (l.type == LockType.SHARED && desired == LockType.EXCLUSIVE) {
                    // Upgrade from SHARED to EXCLUSIVE
                    it.remove();
                    locks.add(new Lock(tid, LockType.EXCLUSIVE));
                }
                // Record the page as locked by the transaction
                txnPages.computeIfAbsent(tid, k -> new HashSet<>()).add(pid);
                return;
            }
        }
        // Add the new lock
        locks.add(new Lock(tid, desired));
        txnPages.computeIfAbsent(tid, k -> new HashSet<>()).add(pid);
    }

    /**
     * Checks if the lock can be granted to the transaction without conflicts.
     */
    private boolean canGrant(TransactionId tid, PageId pid, LockType desired) {
        List<Lock> locks = pageLocks.get(pid);
        if (locks == null || locks.isEmpty()) return true;

        for (Lock l : locks) {
            // If another transaction holds a conflicting lock, lock can't be granted
            if (!l.tid.equals(tid)) {
                if (desired == LockType.EXCLUSIVE || l.type == LockType.EXCLUSIVE) {
                    return false;
                }
            }
        }

        return true;
    }
    /**
     * Returns a set of transactions currently holding locks that conflict with the requested lock.
     */
    private Set<TransactionId> getConflictingHolders(TransactionId tid, PageId pid, LockType desired) {
        Set<TransactionId> blockers = new HashSet<>();
        List<Lock> locks = pageLocks.get(pid);
        if (locks == null) return blockers;

        for (Lock l : locks) {
            if (!l.tid.equals(tid)) {
                if (desired == LockType.EXCLUSIVE || l.type == LockType.EXCLUSIVE) {
                    blockers.add(l.tid); // This transaction blocks our request
                }
            }
        }

        return blockers;
    }

    /**
     * Checks the waits-for graph starting from tid to detect a cycle (deadlock).
     * Uses DFS to traverse the graph and look for cycles.
     */
    private boolean hasCycle(TransactionId tid, Set<TransactionId> visited) {
        // If we've already seen this node in this path, we have a cycle
        if (visited.contains(tid)) return true;
        // Mark this node as visited
        visited.add(tid);
        // Recursively check all transactions this one is waiting for
        Set<TransactionId> neighbors = waitingGraph.getOrDefault(tid, Collections.emptySet());
        for (TransactionId next : neighbors) {
            if (hasCycle(next, visited)) return true;
        }
        // Backtrack
        visited.remove(tid);
        return false;
    }
}
