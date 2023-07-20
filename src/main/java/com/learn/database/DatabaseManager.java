package com.learn.database;

import com.learn.transaction.TransactionManager;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author peiyou
 * @version 1.0
 * @className DatabaseManager
 * @date 2023/7/20 14:10
 **/
public class DatabaseManager {

    private Map<String, Database> databaseMap = new HashMap<>();

    private String path;

    private Lock lock;

    public DatabaseManager(String path) {
        this.lock = new ReentrantLock();
        File file = new File(path);
        if (!file.exists()) {
            //
            throw new RuntimeException("指定的目录不存在.");
        }
        if (!file.isDirectory()) {
            throw new RuntimeException("不是一个有效的路径.");
        }

        File[] dbFiles = file.listFiles();
        if (dbFiles == null) {
            // 没有创建数据库

        } else {
            for (File db: dbFiles) {
                if (db.isDirectory()) {
                    this.findAndInitDatabase(db);
                }
            }
        }
        this.path = path;
    }

    public Database createDatabase(String dbName) {
        lock.lock();
        try {
            if (databaseMap.containsKey(dbName)) {
                throw new RuntimeException("数据库已存在。");
            }
            File file = new File(this.path + File.separator + dbName);
            if (file.exists()) {
                throw new RuntimeException("数据库已存在。");
            }
            boolean mkdir = file.mkdir();
            if (mkdir) {
                Database database = Database.newDatabase(file.getPath(), dbName);
                databaseMap.put(dbName, database);
            } else {
                throw new RuntimeException("创建失败。");
            }
        } finally {
            lock.unlock();
        }
        return databaseMap.get(dbName);
    }

    /**
     * 数据库是一个目录，目录里面包含
     * 1、{databaseName}.xid 事务相关文件
     * 2、xxx.frm 和 xxx.idb 的表文件
     * 3、xxx.log 文件（todo 后面加上日志功能）
     */
    private void findAndInitDatabase(File file) {
        if (!file.isDirectory()) {
            throw new RuntimeException("不是一个有效的数据库.");
        }

        String dbName = file.getName();
        File[] files = file.listFiles();
        if (files == null || files.length == 0) {
            // 无效的数据库
            return;
        }
        String xidFileName = dbName + TransactionManager.XID_SUFFIX;
        for (File f: files) {
            // 查找 .xid 文件
            if (xidFileName.equalsIgnoreCase(f.getName())) {
                Database database = Database.initDatabase(dbName, f);
                databaseMap.put(dbName, database);
                break;
            }
        }
    }

    public void close() throws IOException {
        for (Database database: databaseMap.values()) {
            database.close();
        }
        databaseMap.clear();
    }

    public Database getDatabase(String dbName) {
        if (!databaseMap.containsKey(dbName)) {
            throw new RuntimeException("库不存在，请先创建。");
        }
        return databaseMap.get(dbName);
    }


}
