package com.learn.version;

import com.learn.data.DataManager;
import com.learn.transaction.Transaction;
import com.learn.transaction.TransactionManager;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 版本管理，MVCC的实现
 * @author peiyou
 * @version 1.0
 * @className VersionManager
 * @date 2023/7/20 09:20
 **/
public class VersionManager {

    private DataManager dataManager;

    private Lock lock;

    private Map<Long, Transaction> activeTransaction;

    private TransactionManager transactionManager;

    private LockTable lockTable;

    public VersionManager(DataManager dataManager, TransactionManager transactionManager) {
        this.dataManager = dataManager;
        this.transactionManager = transactionManager;
        this.lock = new ReentrantLock();
        this.activeTransaction = new ConcurrentHashMap<>();
        this.lockTable = new LockTable();
    }
    /**
     *
     * @author Peiyou
     * @date 2023/7/20 10:48
     * @param xid
     * @param uid
     * @return
     */
    public byte[] read(long xid, long uid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();
        if (t.isError()) {
            throw new RuntimeException("读取数据失败，当前事务异常.");
        }

        VersionWrap versionWrap = VersionWrap.load(dataManager, uid);
        try {
            if (versionWrap.isValid()) {
                if (Visibility.isVisible(transactionManager, t, versionWrap)) {
                    return versionWrap.data();
                } else {
                    return null;
                }
            } else {
                return null;
            }
        } finally {
            versionWrap.release();
        }
    }

    // 插入数据
    public long insert(long xid, byte[] data) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if (t.isError()) {
            throw new RuntimeException("读取数据失败，当前事务异常.");
        }

        byte[] raw = VersionWrap.wrapRaw(xid, data);
        return dataManager.insert(raw);
    }

    // 删除数据
    public boolean delete(long xid, long uid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if (t.isError()) {
            throw new RuntimeException("读取数据失败，当前事务异常.");
        }

        VersionWrap versionWrap = VersionWrap.load(dataManager, uid);
        try {
            if(!Visibility.isVisible(transactionManager, t, versionWrap)) {
                return false;
            }
            CountDownLatch latch = null;
            try {
                latch = lockTable.add(xid, uid);
            } catch(Exception e) {
                internAbort(xid, true);
                t.setError(true);
                throw e;
            }

            Long newUid = null;
            if (latch != null) {
                latch.await();
                // 说明上一个线程唤醒了当前线程，上一个线程持有当前线程的资源。
                if (t.commitAfterUpdate(lockTable.lastXid)) {
                    newUid = t.getXidOfUpdateUid(lockTable.lastXid, uid);
                }
            }
            if (newUid != null) {
                versionWrap = VersionWrap.load(dataManager, uid);
            }
            if(versionWrap.getXidMax() == xid) {
                return false;
            }

            if(Visibility.isVersionSkip(transactionManager, t, versionWrap)) {
                internAbort(xid, true);
                t.setError(true);
                throw new RuntimeException("并发更新了数据。uid:" + newUid);
            }
            // 更新数据
            versionWrap.setXidMax(xid);
            return true;
        } finally {
            if (versionWrap != null) {
                versionWrap.release();
            }
        }
    }

    private void internAbort(long xid, boolean autoAborted) {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        activeTransaction.remove(xid);
        lock.unlock();

        lockTable.remove(xid);
        // transactionManager.abort(xid);
    }

    public Map<Long, Transaction> getActiveTransaction() {
        return activeTransaction;
    }

    public long begin(int level, long xid) {
        lock.lock();
        try {
            // long xid = transactionManager.begin();
            Transaction t = Transaction.createTransaction(xid, level, activeTransaction);
            activeTransaction.put(xid, t);
            return xid;
        } finally {
            lock.unlock();
        }
    }

    public void commit(long xid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if (t.isError()) {
            throw new RuntimeException("事务异常。");
        }

        lock.lock();
        t.notifyActivityTransaction(activeTransaction);
        activeTransaction.remove(xid);
        lock.unlock();

        lockTable.remove(xid);
//        transactionManager.commit(xid);
    }

    public void abort(long xid) {
        internAbort(xid, false);
    }

}
