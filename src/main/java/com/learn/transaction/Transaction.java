package com.learn.transaction;

import java.util.HashMap;
import java.util.Map;

/**
 * 事务实体
 * @author peiyou
 * @version 1.0
 * @className Transaction
 * @date 2023/7/19 17:29
 **/
public class Transaction {

    public static final int READ_COMMIT = 0;
    public static final int REPEATABLE = 1;

    public static int SUPER_XID = 0;

    private long xid;

    // 事务隔离级别 0 表示读提交，1表示可重复读
    private int level;

    // 事务开启时，当前有多少事务未提交
    private Map<Long, Boolean> snapshot;

    // 进行中的事务已提交。
    // 进行中的事务修改了哪些数据，因为本质上数据是不会被删除的，修改数据后，只会把旧数据改会无效，并把索引指针指向新数据
    // 如果有多个事务在同时处理同一条数据时，需要让后面处理数据的事务接着新数据继续
    private Map<Long, Map<Long, Long>> snapshotUpdateDataOfUid;


    // 当前事务修改的数据
    private Map<Long, Long> currentUpdateDataOfUid;

    // 事务发生异常了。
    private boolean error;

    public static Transaction createTransaction(long xid, int level, Map<Long, Transaction> active) {
        Transaction transaction = new Transaction();
        transaction.xid = xid;
        transaction.level = level;
        transaction.snapshot = new HashMap<>();
        transaction.snapshotUpdateDataOfUid = new HashMap<>();
        transaction.currentUpdateDataOfUid = new HashMap<>();
        if (xid != SUPER_XID) {
            if (active != null) {
                for (Long x : active.keySet()) {
                    transaction.snapshot.put(x, true);
                }
            }
        }
        return transaction;
    }

    public boolean isInSnapshot(long xid) {
        if(xid == SUPER_XID) {
            return false;
        }
        return snapshot.containsKey(xid);
    }

    public void notifyActivityTransaction(Map<Long, Transaction> activeTransaction) {
        if (this.currentUpdateDataOfUid != null && this.currentUpdateDataOfUid.size() > 0) {
            for (Long xid : activeTransaction.keySet()) {
                Transaction transaction = activeTransaction.get(xid);
                if (transaction != null) {
                    Map<Long, Boolean> map = transaction.snapshot;
                    if (map.containsKey(this.xid)) {
                        transaction.snapshotUpdateDataOfUid.put(this.xid, this.currentUpdateDataOfUid);
                    }
                }
            }
        }
    }

    /**
     * 是否存在在xid更新了数据
     * @author Peiyou
     * @date 2023/7/20 11:32
     * @param xid
     * @return
     */
    public boolean commitAfterUpdate(long xid) {
        if (this.snapshotUpdateDataOfUid != null && this.snapshotUpdateDataOfUid.containsKey(xid)) {
            return true;
        }
        return false;
    }

    /**
     * 获取被xid修改的uid后的最新uid
     * @author Peiyou
     * @date 2023/7/20 12:04
     * @param xid
     * @return long
     */
    public Long getXidOfUpdateUid(long xid, long uid) {
        return snapshotUpdateDataOfUid.get(xid).get(uid);
    }

    public void updateUid(long uid, long newUid) {
        this.currentUpdateDataOfUid.put(uid, newUid);
    }

    public long getXid() {
        return xid;
    }

    public void setXid(long xid) {
        this.xid = xid;
    }

    public int getLevel() {
        return level;
    }

    public void setLevel(int level) {
        this.level = level;
    }

    public boolean isError() {
        return error;
    }

    public void setError(boolean error) {
        this.error = error;
    }
}
