package com.learn.data;

import com.learn.page.Page;
import com.learn.page.PageCache;
import com.learn.page.PageIndex;
import com.learn.page.PageInfo;

/**
 * @author peiyou
 * @version 1.0
 * @className DataManager
 * @date 2023/7/15 16:56
 **/
public class DataManager {

    private PageIndex pageIndex;

    private PageCache pageCache;

    public DataManager(PageCache pageCache) {
        this.pageIndex = new PageIndex();
        this.pageCache = pageCache;
    }

    public long insert(byte[] data) throws Exception {
        byte[] wrap = DataItem.wrap(data);
        PageInfo pageInfo = null;
        while (true) {
            pageInfo = pageIndex.select(wrap.length);
            if (pageInfo == null) {
                // 没有找到合适的页
                Page page = pageCache.newPage(wrap.length);
                pageIndex.add(page.getPageNo(), page.freeSize());
            } else {
                break;
            }
        }
        Page page = pageCache.get(pageInfo.pageNo());
        int offset = page.write(wrap);
        long uid = (long) page.getPageNo();
        uid = uid << 32 | ((long) offset);
        return uid;
    }
}
