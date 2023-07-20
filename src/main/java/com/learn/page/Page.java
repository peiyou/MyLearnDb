package com.learn.page;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * [size][offset][data]
 * @author peiyou
 * @version 1.0
 * @className Page
 * @date 2023/7/12 13:17
 **/
public class Page {
    // 每页的前4个字节是页的长度
    public static final int SIZE_OFFSET = 0;

    public static final int USE_OFFSET_SIZE = 4;

    public static final int DATA_OFFSET = 8;

    // 普通页的大小为16kb，其它页肯定是16kb的整数倍
    public static final int SIZE = 16 * 1024;

    private byte[] data;

    // 是否为脏页
    private boolean dirty;

    // 页内偏移量，数据可插入位置
    private int offset;

    private int size;

    /**
     * 写入数据时需要锁定
     */
    private final Lock pageLock;

    private int pageNo;

    private PageCache pageCache;

    /**
     *
     * @param size 需要的大小
     * @param data 申请下来的容器
     */
    public Page(int size, byte[] data, int pageNo, PageCache pageCache) {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES).putInt(size);
        System.arraycopy(buffer.array(), 0, data, SIZE_OFFSET, 4);
        pageLock = new ReentrantLock();
        this.offset = DATA_OFFSET;
        this.data = data;
        this.dirty = true;
        this.pageNo = pageNo;
        this.pageCache = pageCache;
        this.size = size;
        setOffset();
    }

    private Page(byte[] data, int pageNo, PageCache pageCache) {
        this.data = data;
        this.pageNo = pageNo;
        this.pageCache = pageCache;
        this.dirty = false;
        this.size = data.length;
        pageLock = new ReentrantLock();
    }

    public static Page loadPage(byte[] data, int pageNo, PageCache pageCache) {
        Page page = new Page(data, pageNo, pageCache);
        ByteBuffer buffer = ByteBuffer.wrap(data);
        int size = buffer.getInt();
        page.offset = buffer.getInt();
        return page;
    }
    /**
     * 写入内容，会更新页内偏移量<p/>
     * 返回的值是写入时的开始位置
     * @param bytes
     * @return
     */
    public int write(byte[] bytes) {
        pageLock.lock();
        int startOffset = offset;
        try {
            dirty = true;
            System.arraycopy(bytes, 0, this.data, startOffset, bytes.length);
            offset = startOffset + bytes.length;
            setOffset();
        } finally {
            pageLock.unlock();
        }
        return startOffset;
    }

    /**
     * 更新数据
     * @author Peiyou
     * @date 2023/7/14 09:20
     * @param dataOffset
     * @param updateData
     * @return
     */
    public boolean update(int dataOffset, byte[] updateData) {
        pageLock.lock();
        try {
            dirty = true;
            System.arraycopy(updateData, 0, this.data, dataOffset, updateData.length);
        } finally {
            pageLock.unlock();
        }
        return true;
    }

    public boolean isDirty() {
        return dirty;
    }

    private void setOffset() {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES).putInt(offset);
        System.arraycopy(buffer.array(), 0, data, USE_OFFSET_SIZE, 4);
    }

    /**
     * 获取页在文件中的偏移量
     * @author Peiyou
     * @date 2023/7/12 15:24
     * @param pageNo
     * @return
     */
    public static int pageOffset(int pageNo) {
        return (pageNo - 1) * SIZE;
    }

    public byte[] getData() {
        return data;
    }

    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    public int getPageNo() {
        return pageNo;
    }

    public int pageLength(int size) {
        return (size / Page.SIZE);
    }

    public void release() throws Exception {
        pageCache.release((long)this.pageNo);
    }

    public void force() {
        pageCache.releaseForCache(this);
        this.dirty = false;
    }

    public int freeSize() {
        return size - offset;
    }

    public int getOffset() {
        return this.offset;
    }
}

