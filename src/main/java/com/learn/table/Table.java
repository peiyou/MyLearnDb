package com.learn.table;

import com.learn.data.DataManager;
import com.learn.page.Page;
import com.learn.page.PageCache;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 页按顺序存。
 * 格式
 * [rootIndexUid][size][Column][Column]
 * 索引的根节点,字段数量,字段1,字段2
 * @author peiyou
 * @version 1.0
 * @className Table
 * @date 2023/7/12 13:13
 **/
public class Table {

    public static final String idb = ".idb";
    public static final String frm = ".frm";

    private RandomAccessFile idbFile;

    private PageCache pageCacheIdb;

    private RandomAccessFile frmFile;

    private PageCache pageCacheFrm;

    private List<Column> columns;

    // 主键索引的根节点
    private long rootIndexUid;

    private Lock lock;

    private DataManager dataManager;

    public Table(String path, String name) throws Exception {
        File file = new File(path + File.separator + name + frm);
        if (!file.exists()) {
            throw new RuntimeException(name + "表不存在.");
        }
        if(!file.canRead() || !file.canWrite()) {
            throw new RuntimeException(name + "表不可读写.");
        }
        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(file, "rw");
            fc = raf.getChannel();
            int maxPageNo = (int)(raf.length() / Page.SIZE);
            pageCacheFrm = new PageCache(fc, maxPageNo);
            this.frmFile = raf;
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }

        File f1 = new File(path + File.separator + name + idb);
        if (!f1.exists()) {
            throw new RuntimeException(name + "表不存在.");
        }
        if(!f1.canRead() || !f1.canWrite()) {
            throw new RuntimeException(name + "表不可读写.");
        }
        try {
            raf = new RandomAccessFile(f1, "rw");
            fc = raf.getChannel();
            int maxPageNo = (int)(raf.length() / Page.SIZE);
            pageCacheIdb = new PageCache(fc, maxPageNo);
            this.idbFile = raf;
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        this.loadTableInfo();
        this.lock = new ReentrantLock();
        this.dataManager = new DataManager(pageCacheIdb);
    }

    /**
     * 从frm文件中获取表的信息，列信息，主键信息，root索引信息
     */
    private void loadTableInfo() throws Exception {
        // todo 这里如果字段过多，可能大于1页的情况，暂时先不处理
        Page page = pageCacheFrm.get(1);
        ByteBuffer buffer = ByteBuffer.wrap(page.getData());
        // 页大小
        int pageSize = buffer.getInt();
        // 写偏移
        int pageOffset = buffer.getInt();
        this.rootIndexUid = buffer.getLong();
        this.loadColumns(buffer);
        page.release();
    }

    /**
     * 加载列信息
     */
    private void loadColumns(ByteBuffer buffer) {
        int columnSize = buffer.getInt();
        columns = new ArrayList<>(columnSize);
        for (int i=0; i < columnSize; i++) {
            Column column = Column.instance(buffer);
            this.columns.add(column);
        }
    }

    public void close() throws IOException {
        lock.lock();
        try {
            pageCacheIdb.close();
            pageCacheFrm.close();
            idbFile.close();
            frmFile.close();
        } finally {
            lock.unlock();
        }
    }

    public static Table create(String path, String name, List<Column> columns) throws Exception {
        Bootstrap bootstrap = new Bootstrap(path, name, columns);
        Table table = new Table(path, name);
        return table;
    }

    public long insert(Row row) throws Exception {
        long uid = dataManager.insert(row.getBytes());
        return uid;
    }
}
