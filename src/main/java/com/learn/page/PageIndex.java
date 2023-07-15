package com.learn.page;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 用于选择页面存数据
 * @author peiyou
 * @version 1.0
 * @className PageIndex
 * @date 2023/7/12 14:54
 **/
public class PageIndex {

    // 将一页划成40个区间
    private static final int INTERVALS_NO = 40;
    private static final int THRESHOLD = Page.SIZE / INTERVALS_NO;

    private List<PageInfo>[] lists;
    private Lock lock;

    public PageIndex() {
        lock = new ReentrantLock();
        lists = new List[INTERVALS_NO+1];
        for (int i = 0; i < INTERVALS_NO+1; i ++) {
            lists[i] = new ArrayList<>();
        }
    }

    public PageInfo select(int size) {
        lock.lock();
        try {
            int number = size / THRESHOLD;
            if(number < INTERVALS_NO) number ++;
            while(number <= INTERVALS_NO) {
                if(lists[number].size() == 0) {
                    number ++;
                    continue;
                }
                return lists[number].remove(0);
            }
            return null;
        } finally {
            lock.unlock();
        }
    }

    public void add(int pageNo, int freeSpace) {
        lock.lock();
        try {
            // 可能会有比page本身还大的自由空间
            freeSpace = freeSpace % Page.SIZE;
            int number = freeSpace / THRESHOLD;
            lists[number].add(new PageInfo(pageNo, freeSpace));
        } finally {
            lock.unlock();
        }
    }
}
