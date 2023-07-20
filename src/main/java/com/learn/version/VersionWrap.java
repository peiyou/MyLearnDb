package com.learn.version;

import com.google.common.primitives.Bytes;
import com.learn.data.DataItem;
import com.learn.data.DataManager;

import java.nio.ByteBuffer;

/**
 * 对数据进行包裹，用于支持版本控制
 * @author peiyou
 * @version 1.0
 * @className VersionWrap
 * @date 2023/7/20 09:37
 **/
public class VersionWrap {

    // 创建数据时的事务id
    private static final int OF_XMIN = 0;
    // 删除数据时的事务ID
    private static final int OF_XMAX = OF_XMIN+8;
    // 数据
    private static final int OF_DATA = OF_XMAX+8;

    private long uid;

    private DataItem dataItem;

    private DataManager dataManager;

    private long xidMin;

    private long xidMax;

    private static VersionWrap getInstance(DataManager dataManager, DataItem dataItem, long uid) {
        VersionWrap wrap = new VersionWrap();
        wrap.uid = uid;
        wrap.dataItem = dataItem;
        wrap.dataManager = dataManager;
        ByteBuffer buffer = ByteBuffer.wrap(dataItem.getData());
        wrap.xidMin = buffer.getLong();
        wrap.xidMax = buffer.getLong();
        return wrap;
    }

    public static VersionWrap load(DataManager dataManager, long uid) throws Exception {
        DataItem dataItem = dataManager.get(uid);
        return getInstance(dataManager, dataItem, uid);
    }

    public static byte[] wrapRaw(long xid, byte[] data) {
        byte[] xmin = ByteBuffer.allocate(Long.BYTES).putLong(xid).array();
        byte[] xmax = ByteBuffer.allocate(Long.BYTES).putLong(0).array();
        return Bytes.concat(xmin, xmax, data);
    }

    public void release() throws Exception {
        dataManager.release(uid);
    }

    public byte[] data() {
        dataItem.getReadLock().lock();
        try {
            byte[] sa = dataItem.getData();
            byte[] data = new byte[sa.length - OF_DATA];
            System.arraycopy(sa, OF_DATA, data, 0, data.length);
            return data;
        } finally {
            dataItem.getReadLock().unlock();
        }
    }

    public long getXidMin() {
        return xidMin;
    }

    public long getXidMax() {
        return xidMax;
    }

    public boolean isValid() {
        return dataItem.isValid();
    }

    public void setXidMax(long xid) {
        dataItem.getWriteLock().lock();
        try {
            byte[] data = this.data();
            byte[] xidMax = ByteBuffer.allocate(Long.BYTES).putLong(xid).array();
            System.arraycopy(xidMax, 0, data, OF_XMAX, xidMax.length);
            dataItem.update(data);
        } finally {
            dataItem.getWriteLock().unlock();
        }
    }
}
