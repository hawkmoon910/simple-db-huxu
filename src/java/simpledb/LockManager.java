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
            try {
                wait();
            } catch (InterruptedException e) {
                throw new TransactionAbortedException();
            }
        }

        // Grant lock
        List<Lock> locks = pageLocks.computeIfAbsent(pid, k -> new ArrayList<>());

        // Upgrade if necessary
        boolean upgraded = false;
        for (Lock l : locks) {
            if (l.tid.equals(tid)) {
                if (l.type == LockType.SHARED && desired == LockType.EXCLUSIVE) {
                    locks.remove(l);
                    locks.add(new Lock(tid, LockType.EXCLUSIVE));
                    upgraded = true;
                }
                break;
            }
        }

        if (!holdsLock(tid, pid)) {
            locks.add(new Lock(tid, desired));
        }

        txnPages.computeIfAbsent(tid, k -> new HashSet<>()).add(pid);
    }

    private boolean canGrant(TransactionId tid, PageId pid, LockType desired) {
        List<Lock> locks = pageLocks.get(pid);
        if (locks == null || locks.isEmpty()) return true;

        for (Lock l : locks) {
            if (l.tid.equals(tid)) {
                if (l.type == LockType.SHARED && desired == LockType.EXCLUSIVE && locks.size() == 1) {
                    return true; // Upgrade
                }
                if (l.type == desired) {
                    return true; // Already has it
                }
            }
        }

        if (desired == LockType.SHARED) {
            // Grant shared if no exclusive
            return locks.stream().noneMatch(l -> l.type == LockType.EXCLUSIVE);
        } else {
            // Exclusive: only if no other locks
            return false;
        }
    }
}
