package com.learn.data;

import com.google.common.primitives.Bytes;
import com.learn.page.Page;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * [valid][size][data]
 * 数据项，格式如上
 * @author peiyou
 * @version 1.0
 * @className DataItem
 * @date 2023/7/13 16:23
 **/
public class DataItem {
    private static final int VALID = 0;
    private static final int SIZE = VALID + 1;
    public static final int DATA = SIZE + 4;

    /**
     * 在页中的偏移量
     */
    private final int offset;

    private final int size;

    // 对于更新时，用来保存旧值的
    private byte[] oldRaw;

    private Page page;

    private byte valid;

    private Lock rLock;
    private Lock wLock;

    public DataItem(int offset, Page page) {
        this.offset = offset;
        this.page = page;
        ByteBuffer buffer = ByteBuffer.wrap(page.getData());
        buffer.position(offset);
        this.valid = buffer.get();
        this.size = buffer.getInt();
        ReadWriteLock lock = new ReentrantReadWriteLock();
        rLock = lock.readLock();
        wLock = lock.writeLock();
    }

    /**
     * 插入数据返回数据在页中的偏移量
     * @author Peiyou
     * @date 2023/7/14 09:12
     * @param data
     * @return int
     */
    public int insert(byte[] data) {
        byte[] wrap = wrap(data);
        return page.write(wrap);
    }

    /**
     * 更新有效性
     * @author Peiyou
     * @date 2023/7/14 09:22
     * @param valid
     * @return
     */
    public void updateValid(boolean valid) {
        setOldRaw();
        byte[] newData = new byte[size];
        System.arraycopy(oldRaw, 0, newData, 0, size);
        newData[0] = valid ? (byte)1: (byte) 0;
        this.valid = newData[0];
        page.update(offset, newData);
    }

    private void setOldRaw() {
        oldRaw = new byte[size];
        ByteBuffer buffer = ByteBuffer.wrap(page.getData());
        buffer.position(offset);
        byte validByte = buffer.get();
        int size = buffer.getInt();
        buffer.get(oldRaw);
    }
    public static byte[] wrap(byte[] data) {
        byte[] valid = new byte[]{(byte)1};
        byte[] size = ByteBuffer.allocate(Integer.BYTES).putInt(data.length).array();
        return Bytes.concat(valid, size, data);
    }

    public boolean isValid() {
        return this.valid == (byte)1;
    }

    public void release() throws Exception {
        page.release();
    }

   public void force() {
        page.force();
   }

    public byte[] getData() {
        byte[] data = new byte[size];
        System.arraycopy(page.getData(), offset + DATA, data, 0, data.length);
        return data;
    }

    public void update(byte[] data) {
        setOldRaw();
        page.update(offset + DATA, data);
    }

    public Lock getReadLock() {
        return rLock;
    }

    public Lock getWriteLock() {
        return wLock;
    }
}
