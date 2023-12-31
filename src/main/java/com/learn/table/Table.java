package com.learn.table;

import com.learn.btree.BPlusTree;
import com.learn.btree.Node;
import com.learn.data.DataItem;
import com.learn.data.DataManager;
import com.learn.database.Database;
import com.learn.page.Page;
import com.learn.page.PageCache;
import com.learn.transaction.Transaction;
import com.learn.transaction.TransactionManager;
import com.learn.value.Value;
import com.learn.version.VersionManager;

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

    private final String tableName;

    private RandomAccessFile idbFile;

    private PageCache pageCacheIdb;

    private RandomAccessFile frmFile;

    private PageCache pageCacheFrm;

    private List<Column> columns;

    // 主键索引的根节点
    private long rootIndexUid;

    private Lock lock;

    private DataManager dataManager;

    private BPlusTree bPlusTree;

    private VersionManager versionManager;

    private TransactionManager transactionManager;

    public Table(String path, String name, TransactionManager transactionManager) throws Exception {
        this.tableName = name;
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
        this.dataManager = new DataManager(pageCacheIdb);
        this.lock = new ReentrantLock();

        this.loadTableInfo();
        this.bPlusTree = new BPlusTree(this.dataManager, this.pageCacheIdb, this.rootIndexUid);
        this.transactionManager = transactionManager;
        this.versionManager = new VersionManager(this.dataManager, this.transactionManager);
        transactionManager.addVersionManager(versionManager);
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
        if (rootIndexUid > 0) {
            DataItem dataItem = dataManager.get(rootIndexUid);
            this.rootIndexUid = ByteBuffer.wrap(dataItem.getData()).getLong();
        }
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

    public static Table create(String path, String name, List<Column> columns, TransactionManager transactionManager) throws Exception {
        Bootstrap bootstrap = new Bootstrap(path, name, columns);
        Table table = new Table(path, name, transactionManager);
        // 往idb的第一页中写入 root 占位
        Page page = table.pageCacheIdb.newPage(Page.SIZE);
        Node node = BPlusTree.newNode(table.pageCacheIdb, table.dataManager);
        // 前8个字符表示root的uid
        int offset = page.write(DataItem.wrap(ByteBuffer.allocate(Long.BYTES).putLong(node.getUid()).array()));
        table.rootIndexUid = (1L << 32 | (long)offset);
        // 更新
        Page frmPage = table.pageCacheFrm.get(1);
        frmPage.update(Page.DATA_OFFSET, ByteBuffer.allocate(Long.BYTES).putLong(table.rootIndexUid).array());
        frmPage.release();
        return table;
    }

    public long insert(long xid, Row row) throws Exception {
        Value key = null;
        for (Column column: columns) {
            if(column.isPrimaryKey()) {
                key = row.get(column.index());
            }
        }
        if (key == null || key.isNull()) {
            throw new RuntimeException("主键不能为空，或没有主键.");
        }
        Row select = this.select(xid, key);
        if (select != null) {
            // 主键冲突
            throw new RuntimeException("插入重复的主键。");
        }
        long uid = versionManager.insert(xid, row.getBytes());
        bPlusTree.add(key, uid);
        return uid;
    }

    public Row select(long xid, Value key) throws Exception {
        Long uid = bPlusTree.search(key);
        if (uid == null) {
            return null;
        }
        byte[] data = versionManager.read(xid, uid);
        if (data == null) {
            return null;
        } else {
            return new Row(ByteBuffer.wrap(data), columns);
        }
    }

    public boolean delete(long xid, Value key) throws Exception {
        long uid = bPlusTree.search(key);
        return versionManager.delete(xid, uid);
    }


    public boolean update(long xid, Row row) throws Exception {
        Value key = null;
        int indexKey = -1;
        for (Column column: this.columns) {
            if (column.isPrimaryKey()) {
                indexKey = column.index();
                break;
            }
        }
        if (indexKey == -1) {
            // 异常
            throw new RuntimeException("不存在主键.");
        }
        key = row.get(indexKey);
        long uid = bPlusTree.search(key);
        byte[] data = versionManager.read(xid, uid);
        Row old = null;
        if (data != null){
            old = new Row(ByteBuffer.wrap(data), columns);
        }

        if (old == null) {
            return false;
        }
        this.delete(xid, key);

        for (int i = 0; i < this.columns.size(); i++) {
            if (row.get(i) != null) {
                old.setCol(i, row.get(i));
            }
        }
        long newUid = this.insert(xid, old);
        Transaction transaction = versionManager.getActiveTransaction().get(xid);
        transaction.updateUid(uid, newUid);
        bPlusTree.add(key, newUid);
        return true;
    }

    public boolean dropTable(Database database) {
        new File(database.getPath() + File.separator + tableName + Table.frm).delete();
        new File(database.getPath() + File.separator + tableName + Table.idb).delete();
        return true;
    }
}
