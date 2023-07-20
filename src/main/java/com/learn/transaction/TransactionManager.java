package com.learn.transaction;

import com.learn.version.VersionManager;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 事务管理器
 * @author peiyou
 * @version 1.0
 * @className TransactionManager
 * @date 2023/7/19 17:29
 **/
public class TransactionManager {
    // XID文件头长度
    public static final int LEN_XID_HEADER_LENGTH = 8;

    // 每个事务的占用长度
    private static final int XID_FIELD_SIZE = 1;

    // 事务的三种状态
    private static final byte FIELD_TRAN_ACTIVE   = 0;
    private static final byte FIELD_TRAN_COMMITTED = 1;
    private static final byte FIELD_TRAN_ABORTED  = 2;

    // 事务文件的后缀，应该在创建数据库的时候创建出对应的事务文件
    public static final String XID_SUFFIX = ".xid";

    private RandomAccessFile file;
    private FileChannel fileChannel;
    private long xidCounter;
    private Lock counterLock;

    private List<VersionManager> versionManagerList;

    public TransactionManager(RandomAccessFile file, FileChannel fileChannel) {
        this.file = file;
        this.fileChannel = fileChannel;
        counterLock = new ReentrantLock();
        checkXIDCounter();
        versionManagerList = new ArrayList<>();
    }

    /**
     * 检查XID文件是否合法
     * 读取XID_FILE_HEADER中的xidcounter，根据它计算文件的理论长度，对比实际长度
     */
    private void checkXIDCounter() {
        long fileLen = 0;
        try {
            fileLen = file.length();
        } catch (IOException e1) {
            throw new RuntimeException("事务文件坏的");
        }
        if(fileLen < LEN_XID_HEADER_LENGTH) {
            throw new RuntimeException("事务文件坏的");
        }

        ByteBuffer buf = ByteBuffer.allocate(LEN_XID_HEADER_LENGTH);
        try {
            fileChannel.position(0);
            fileChannel.read(buf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        buf.position(0);
        this.xidCounter = buf.getLong();
        long end = getXidPosition(this.xidCounter + 1);
        if(end != fileLen) {
            throw new RuntimeException("事务文件坏的");
        }
    }

    private long getXidPosition(long xid) {
        return LEN_XID_HEADER_LENGTH + (xid-1)*XID_FIELD_SIZE;
    }

    // 更新xid事务的状态为status
    private void updateXID(long xid, byte status) {
        long offset = getXidPosition(xid);
        byte[] tmp = new byte[XID_FIELD_SIZE];
        tmp[0] = status;
        ByteBuffer buf = ByteBuffer.wrap(tmp);
        try {
            fileChannel.position(offset);
            fileChannel.write(buf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try {
            fileChannel.force(false);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // 开始一个事务，并返回XID
    public long begin(int level) {
        counterLock.lock();
        try {
            long xid = xidCounter + 1;
            updateXID(xid, FIELD_TRAN_ACTIVE);
            incrXIDCounter();
            for (VersionManager versionManager: versionManagerList) {
                versionManager.begin(level, xid);
            }
            return xid;
        } finally {
            counterLock.unlock();
        }
    }

    // 将XID加一，并更新XID Header
    private void incrXIDCounter() {
        xidCounter ++;
        ByteBuffer buf = ByteBuffer.allocate(Long.BYTES).putLong(xidCounter);
        buf.position(0);
        try {
            fileChannel.position(0);
            fileChannel.write(buf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try {
            fileChannel.force(false);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // 提交XID事务
    public void commit(long xid) throws Exception {
        for (VersionManager versionManager: versionManagerList) {
            versionManager.commit(xid);
        }
        updateXID(xid, FIELD_TRAN_COMMITTED);
    }

    // 回滚XID事务
    public void abort(long xid) {
        for (VersionManager versionManager: versionManagerList) {
            versionManager.abort(xid);
        }
        updateXID(xid, FIELD_TRAN_ABORTED);
    }

    // 检测XID事务是否处于status状态
    private boolean checkXID(long xid, byte status) {
        long offset = getXidPosition(xid);
        ByteBuffer buf = ByteBuffer.wrap(new byte[XID_FIELD_SIZE]);
        try {
            fileChannel.position(offset);
            fileChannel.read(buf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return buf.array()[0] == status;
    }

    public boolean isActive(long xid) {
        if(xid == Transaction.SUPER_XID) return false;
        return checkXID(xid, FIELD_TRAN_ACTIVE);
    }

    public boolean isCommitted(long xid) {
        if(xid == Transaction.SUPER_XID) return true;
        return checkXID(xid, FIELD_TRAN_COMMITTED);
    }

    public boolean isAborted(long xid) {
        if(xid == Transaction.SUPER_XID) return false;
        return checkXID(xid, FIELD_TRAN_ABORTED);
    }

    public void close() {
        try {
            fileChannel.close();
            file.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void addVersionManager(VersionManager versionManager) {
        counterLock.lock();
        try {
            this.versionManagerList.add(versionManager);
        } finally {
            counterLock.unlock();
        }
    }
}
