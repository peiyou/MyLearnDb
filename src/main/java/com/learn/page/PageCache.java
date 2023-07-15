package com.learn.page;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author peiyou
 * @version 1.0
 * @className PageCache
 * @date 2023/7/12 13:17
 **/
public class PageCache {

    private Map<Integer, Page> cachePage = new ConcurrentHashMap<>();

    // 有几个引用了同一个资源，引用数为0的资源是可以释放的
    private Map<Integer, Integer> references;

    // 正在从文件中获取page页
    private Map<Integer, Boolean> getting;

    private Lock lock;

    private FileChannel fileChannel;

    // 当前的最大页面号，每次申请页的时候更新
    private int maxPageNo;

    // 创建了页，但是页未使用完，可以放在这里
    private PageIndex pageIndex;

    public PageCache(FileChannel fileChannel, int maxPageNo) {
        this.fileChannel = fileChannel;
        this.references = new ConcurrentHashMap<>();
        this.getting = new ConcurrentHashMap<>();
        lock = new ReentrantLock();
        this.maxPageNo = maxPageNo;
        pageIndex = new PageIndex();
    }

    /**
     * 关闭缓存，写回所有资源
     */
    public void close() {
        lock.lock();
        try {
            Set<Integer> keys = cachePage.keySet();
            for (int key : keys) {
                Page obj = cachePage.get(key);
                releaseForCache(obj);
                references.remove(key);
                cachePage.remove(key);
            }
        } finally {
            lock.unlock();
        }
    }

    public Page get(int pageNo) throws Exception {
        while(true) {
            lock.lock();
            if(getting.containsKey(pageNo)) {
                // 请求的资源正在被其他线程获取
                lock.unlock();
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    continue;
                }
                continue;
            }

            if(cachePage.containsKey(pageNo)) {
                // 资源在缓存中，直接返回
                Page page = cachePage.get(pageNo);
                references.put(pageNo, references.get(pageNo) + 1);
                lock.unlock();
                return page;
            }

            // 尝试获取该资源
            getting.put(pageNo, true);
            lock.unlock();
            break;
        }

        Page page = null;
        try {
            page = getForCache(pageNo);
        } catch(Exception e) {
            lock.lock();
            getting.remove(pageNo);
            lock.unlock();
            throw e;
        }

        lock.lock();
        getting.remove(pageNo);
        cachePage.put(pageNo, page);
        references.put(pageNo, 1);
        lock.unlock();

        return page;
    }

    /**
     * 当资源不在缓存时的获取行为
     */
    private Page getForCache(int pageNo) throws Exception {
        lock.lock();
        try {
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
     * 强行释放一个缓存
     */
    protected void release(int pageNo) throws Exception {
        lock.lock();
        try {
            int ref = references.get(pageNo)-1;
            if(ref == 0) {
                Page page = this.get(pageNo);
                releaseForCache(page);
                references.remove(pageNo);
                cachePage.remove(pageNo);
            } else {
                references.put(pageNo, ref);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 当资源被驱逐时的写回行为
     */
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
        return page;
    }
}
