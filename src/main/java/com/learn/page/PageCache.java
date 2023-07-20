package com.learn.page;

import com.learn.cache.AbstractCache;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author peiyou
 * @version 1.0
 * @className PageCache
 * @date 2023/7/12 13:17
 **/
public class PageCache extends AbstractCache<Page> {

    private FileChannel fileChannel;

    // 当前的最大页面号，每次申请页的时候更新
    private int maxPageNo;

    // 创建了页，但是页未使用完，可以放在这里
    private PageIndex pageIndex;

    public PageCache(FileChannel fileChannel, int maxPageNo) {
        super();
        this.fileChannel = fileChannel;
        this.maxPageNo = maxPageNo;
        pageIndex = new PageIndex();
    }


    /**
     * 当资源不在缓存时的获取行为
     */
    @Override
    public Page getForCache(long uid) throws Exception {
        lock.lock();
        try {
            int pageNo = (int) uid;
            int offset = Page.pageOffset(pageNo);
            fileChannel.position(offset);
            // 先读取这个页的大小
            ByteBuffer sizeBuf = ByteBuffer.allocate(Integer.BYTES);
            fileChannel.read(sizeBuf);
            sizeBuf.position(0);
            int size = sizeBuf.getInt();
            fileChannel.position(offset);
            ByteBuffer pageData = ByteBuffer.allocate(size);
            fileChannel.read(pageData);
            return Page.loadPage(pageData.array(), pageNo, this);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 当资源被驱逐时的写回行为
     */
    @Override
    public void releaseForCache(Page page) {
        lock.lock();
        try {
            if (page.isDirty()) {
                fileChannel.position(Page.pageOffset(page.getPageNo()));
                ByteBuffer buf = ByteBuffer.wrap(page.getData());
                fileChannel.write(buf);
                fileChannel.force(false);
            }
            page.setDirty(false);
        } catch (IOException e) {
           throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
    }

    public Page newPage(int needSize) {
        int pageNo = maxPageNo;
        int num = needSize / Page.SIZE;
        pageNo += num;
        int mod = needSize % Page.SIZE;
        int allowSize = num * Page.SIZE;
        if (mod > 0) {
            allowSize += Page.SIZE;
            pageNo++;
        }
        Page page = new Page(allowSize, ByteBuffer.allocate(allowSize).array(), pageNo, this);
        releaseForCache(page);
        maxPageNo = pageNo;
        return page;
    }
}
