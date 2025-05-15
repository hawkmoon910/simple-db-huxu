package simpledb;

import java.util.*;

public class LockManager {
    private enum LockType { SHARED, EXCLUSIVE }

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

    public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
        List<Lock> locks = pageLocks.get(pid);
        if (locks == null) return false;
        for (Lock l : locks) {
            if (l.tid.equals(tid)) return true;
        }
        return false;
    }

    public synchronized void releaseLock(TransactionId tid, PageId pid) {
        List<Lock> locks = pageLocks.get(pid);
        if (locks != null) {
            locks.removeIf(l -> l.tid.equals(tid));
            if (locks.isEmpty()) pageLocks.remove(pid);
        }
        Set<PageId> pages = txnPages.get(tid);
        if (pages != null) {
            pages.remove(pid);
            if (pages.isEmpty()) txnPages.remove(tid);
        }
        waitingGraph.remove(tid); // Clear wait-for info
        for (Set<TransactionId> waiters : waitingGraph.values()) {
            waiters.remove(tid);
        }
        notifyAll(); // wake up waiting threads
    }

    public synchronized void releaseAllLocks(TransactionId tid) {
        Set<PageId> pages = txnPages.getOrDefault(tid, new HashSet<>());
        for (PageId pid : new HashSet<>(pages)) {
            releaseLock(tid, pid);
        }
    }

    public synchronized void acquireLock(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException {

        LockType desired = (perm == Permissions.READ_ONLY) ? LockType.SHARED : LockType.EXCLUSIVE;

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
                wait();
            } catch (InterruptedException e) {
                throw new TransactionAbortedException();
            }
            // Clear waits-for entries after waking up
            waitingGraph.getOrDefault(tid, new HashSet<>()).clear();
        }

        // Grant lock
        List<Lock> locks = pageLocks.computeIfAbsent(pid, k -> new ArrayList<>());

        // Upgrade logic
        for (Iterator<Lock> it = locks.iterator(); it.hasNext();) {
            Lock l = it.next();
            if (l.tid.equals(tid)) {
                if (l.type == LockType.SHARED && desired == LockType.EXCLUSIVE) {
                    it.remove();
                    locks.add(new Lock(tid, LockType.EXCLUSIVE));
                }
                txnPages.computeIfAbsent(tid, k -> new HashSet<>()).add(pid);
                return;
            }
        }

        locks.add(new Lock(tid, desired));
        txnPages.computeIfAbsent(tid, k -> new HashSet<>()).add(pid);
    }

    private boolean canGrant(TransactionId tid, PageId pid, LockType desired) {
        List<Lock> locks = pageLocks.get(pid);
        if (locks == null || locks.isEmpty()) return true;

        for (Lock l : locks) {
            if (!l.tid.equals(tid)) {
                // Another transaction holds a conflicting lock
                if (desired == LockType.EXCLUSIVE || l.type == LockType.EXCLUSIVE) {
                    return false;
                }
            }
        }

        return true;
    }

    // Returns all conflicting transactions currently holding locks on the page
    private Set<TransactionId> getConflictingHolders(TransactionId tid, PageId pid, LockType desired) {
        Set<TransactionId> blockers = new HashSet<>();
        List<Lock> locks = pageLocks.get(pid);
        if (locks == null) return blockers;

        for (Lock l : locks) {
            if (!l.tid.equals(tid)) {
                if (desired == LockType.EXCLUSIVE || l.type == LockType.EXCLUSIVE) {
                    blockers.add(l.tid);
                }
            }
        }

        return blockers;
    }

    // Cycle detection via DFS
    private boolean hasCycle(TransactionId tid, Set<TransactionId> visited) {
        if (visited.contains(tid)) return true;
        visited.add(tid);

        Set<TransactionId> neighbors = waitingGraph.getOrDefault(tid, Collections.emptySet());
        for (TransactionId next : neighbors) {
            if (hasCycle(next, visited)) return true;
        }

        visited.remove(tid);
        return false;
    }
}
