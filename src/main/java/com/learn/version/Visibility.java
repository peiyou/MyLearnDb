package com.learn.version;

import com.learn.transaction.Transaction;
import com.learn.transaction.TransactionManager;

/**
 * 可见性
 * @author peiyou
 * @version 1.0
 * @className Visibility
 * @date 2023/7/20 09:19
 **/
public class Visibility {

    public static boolean isVersionSkip(TransactionManager tm, Transaction t, VersionWrap e) {
        long xmax = e.getXidMax();
        if(t.getLevel() == Transaction.READ_COMMIT) {
            return false;
        } else {
            return tm.isCommitted(xmax) && (xmax > t.getXid() || t.isInSnapshot(xmax));
        }
    }

    public static boolean isVisible(TransactionManager tm, Transaction t, VersionWrap e) {
        if(t.getLevel() == Transaction.READ_COMMIT) {
            return readCommitted(tm, t, e);
        } else {
            return repeatableRead(tm, t, e);
        }
    }

    /**
     * 读提交，只要事务已经提交，那么在别的事务中就可以读取
     * 会产生不可重复读（别的事务删除了数据）
     * 幻读 (别的事务添加了数据)
     * @author Peiyou
     * @date 2023/7/20 11:10
     * @param tm
     * @param t
     * @param e
     * @return
     */
    private static boolean readCommitted(TransactionManager tm, Transaction t, VersionWrap e) {
        long xid = t.getXid();
        long xmin = e.getXidMin();
        long xmax = e.getXidMax();
        // 是当前事务
        if(xmin == xid && xmax == 0) return true;
        // 如果插入数据的事务已提交
        if(tm.isCommitted(xmin)) {
            // 不存在删除数据的事务时
            if(xmax == 0) return true;

            // 不是本次事务删除的数据
            if(xmax != xid) {
                // 删除数据的事务未提交
                if(!tm.isCommitted(xmax)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean repeatableRead(TransactionManager tm, Transaction t,  VersionWrap e) {
        long xid = t.getXid();
        long xmin = e.getXidMin();
        long xmax = e.getXidMax();
        // 是当前事务
        if(xmin == xid && xmax == 0) return true;

        /**
         * 1、插入数据的事务已提交
         * 2、插入数据的事务在本次事务之前
         * 3、
         */
        if(tm.isCommitted(xmin) && xmin < xid && !t.isInSnapshot(xmin)) {
            if(xmax == 0) return true;
            if(xmax != xid) {
                if(!tm.isCommitted(xmax) || xmax > xid || t.isInSnapshot(xmax)) {
                    return true;
                }
            }
        }
        return false;
    }
}
