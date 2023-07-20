package com.learn.database;

import com.learn.table.Column;
import com.learn.table.Table;
import com.learn.transaction.TransactionManager;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author peiyou
 * @version 1.0
 * @className Database
 * @date 2023/7/20 14:10
 **/
public class Database {

    private String name;

    private Map<String, Table> tableInfo = new ConcurrentHashMap<>();

    private TransactionManager transactionManager;

    private Lock lock;

    private String path;

    private Database() {
        this.lock = new ReentrantLock();
    }

    public static Database newDatabase(String path, String dbName) {
        String filePath = path + File.separator + dbName + TransactionManager.XID_SUFFIX;
        File file = new File(filePath);
        if (file.exists()) {
            throw new RuntimeException(".xid文件已存在.");
        }
        try {
            if (!file.createNewFile()) {
                throw new RuntimeException(".xid文件已存在.");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        if (!file.canRead() || !file.canWrite()) {
            throw new RuntimeException("文件不可读写.");
        }

        try (FileOutputStream outputStream = new FileOutputStream(file)) {
            byte[] data = ByteBuffer.allocate(Long.BYTES).putLong(0).array();
            outputStream.write(data);
            outputStream.flush();
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return initDatabase(dbName, file);
    }

    public static Database initDatabase(String dbName ,File xidFile) {
        if (!xidFile.canRead() || !xidFile.canWrite()) {
            throw new RuntimeException(xidFile.getName() + "文件无法读写。");
        }
        Database database = new Database();
        database.name = dbName;
        try {
            RandomAccessFile raf = new RandomAccessFile(xidFile, "rw");
            FileChannel fileChannel = raf.getChannel();
            database.transactionManager = new TransactionManager(raf, fileChannel);
        } catch (IOException e) {
            throw  new RuntimeException(e);
        }
        database.path = xidFile.getParent();
        return database;
    }

    public TransactionManager getTransactionManager() {
        return transactionManager;
    }

    public Table getTable(String tableName) {
        Table table = tableInfo.get(tableName);
        if (table == null) {
            // 如果表未加载，去数据库目录下载入
            lock.lock();
            try {
                Table temp = new Table(this.path, tableName, transactionManager);
                tableInfo.put(tableName, temp);
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                lock.unlock();
            }
            table = tableInfo.get(tableName);
        }
        return table;
    }

    public Table createTable(String tableName, List<Column> columns) throws Exception {
        lock.lock();
        try {
            Table table = Table.create(this.path, tableName, columns, this.transactionManager);
            tableInfo.put(tableName, table);
        } finally {
            lock.unlock();
        }
        return tableInfo.get(tableName);
    }

    public void close() throws IOException {
        this.lock.lock();
        try {
            for (Table table: tableInfo.values()) {
                table.close();
            }
            transactionManager.close();
        } finally {
            this.lock.unlock();
        }
    }

    public boolean dropTable(String tableName) {
        Table table = tableInfo.get(tableName);
        if (table == null) {
            return false;
        } else {
            lock.lock();
            try {
                table.close();
                return table.dropTable(this);
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                lock.unlock();
            }
        }
    }

    public String getPath() {
        return this.path;
    }
}
