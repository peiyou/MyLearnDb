package com.learn.cache;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 缓存相关的抽象类
 * page页与dataItem应该是个可以上锁的对象
 * 如果只有page可以缓存，那读写锁时，只能针对页面
 * 如果要对行记录也可以上锁，那就需要缓存dataItem，只有是同一对象，才能保证是同一把锁
 * @author peiyou
 * @version 1.0
 * @className AbstractCache
 * @date 2023/7/20 10:06
 **/
public abstract class AbstractCache<T> {
    private Map<Long, T> cache = new ConcurrentHashMap<>();

    // 有几个引用了同一个资源，引用数为0的资源是可以释放的
    private Map<Long, Integer> references;

    // 正在从文件中获取page页
    private Map<Long, Boolean> getting;

    protected Lock lock;

    public AbstractCache() {
        this.references = new ConcurrentHashMap<>();
        this.getting = new ConcurrentHashMap<>();
        lock = new ReentrantLock();
    }

    public T get(long uid) throws Exception {
        while(true) {
            lock.lock();
            if(getting.containsKey(uid)) {
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

            if(cache.containsKey(uid)) {
                // 资源在缓存中，直接返回
                T obj = cache.get(uid);
                references.put(uid, references.get(uid) + 1);
                lock.unlock();
                return obj;
            }

            // 尝试获取该资源
            getting.put(uid, true);
            lock.unlock();
            break;
        }

        T obj = null;
        try {
            obj = getForCache(uid);
        } catch(Exception e) {
            lock.lock();
            getting.remove(uid);
            lock.unlock();
            throw e;
        }

        lock.lock();
        getting.remove(uid);
        cache.put(uid, obj);
        references.put(uid, 1);
        lock.unlock();

        return obj;
    }

    public void release(long uid) throws Exception {
        lock.lock();
        try {
            int ref = references.get(uid)-1;
            if(ref == 0) {
                T obj = this.get(uid);
                releaseForCache(obj);
                references.remove(uid);
                cache.remove(uid);
            } else {
                references.put(uid, ref);
            }
        } finally {
            lock.unlock();
        }
    }

    protected abstract T getForCache(long pageNo) throws Exception;

    public abstract void releaseForCache(T obj);

    public void close() {
        lock.lock();
        try {
            Set<Long> keys = cache.keySet();
            for (long key : keys) {
                T obj = cache.get(key);
                releaseForCache(obj);
                references.remove(key);
                cache.remove(key);
            }
        } finally {
            lock.unlock();
        }
    }
}
