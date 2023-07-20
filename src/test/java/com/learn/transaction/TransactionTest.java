package com.learn.transaction;

import com.google.common.primitives.Bytes;
import com.learn.database.Database;
import com.learn.database.DatabaseManager;
import com.learn.table.Column;
import com.learn.table.Row;
import com.learn.table.Table;
import com.learn.value.Value;
import com.learn.value.ValueDouble;
import com.learn.value.ValueInt;
import com.learn.value.ValueLong;
import com.learn.value.ValueString;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author peiyou
 * @version 1.0
 * @className TransactionTest
 * @date 2023/7/20 16:46
 **/
public class TransactionTest {

    private DatabaseManager databaseManager;

    private void init() {
        String path = "/Users/peiyou/myLearDb";
        databaseManager = new DatabaseManager(path);
    }
    @Test
    public void createDb() throws Exception {
        init();
        databaseManager.createDatabase("test_transaction");
    }

    private Table createTable(Database database, String tableName) throws Exception {
        database.dropTable(tableName);
        List<Column> columns = new ArrayList<>();
        columns.add(new Column("id", 0, Value.LONG, false, true));
        columns.add(new Column("name", 1, Value.STRING, true, false));
        return database.createTable(tableName, columns);
    }

    /**
     * 插入数据，但是不提交事务
     * @author Peiyou
     * @date 2023/7/20 16:48
     * @return void
     */
    @Test
    public void insertButNotCommit() throws Exception {
        /**
         *
         * @author Peiyou
         * @date 2023/7/20 16:48
         * @return void
         */

        init();
        String dbName = "test_transaction";
        String tableName = "test";
        Database database = databaseManager.getDatabase(dbName);
        Table table = this.createTable(database, tableName);

        List<Column> columns = new ArrayList<>();
        columns.add(new Column("id", 0, Value.LONG, false, true));
        columns.add(new Column("name", 1, Value.STRING, true, false));
        List<Row> rowList = new ArrayList<>();
        for (int i = 0; i < 10; i ++) {
            Value id = new ValueLong(i + 1, false);
            Value name = new ValueString("张三" + i);
            byte[] rowBytes = Bytes.concat(id.getInputBytes(), name.getInputBytes());
            Row row = new Row(ByteBuffer.wrap(rowBytes), columns);
            rowList.add(row);
        }
        List<Long> uidList = new ArrayList<>();
        TransactionManager transactionManager = database.getTransactionManager();
        int level = Transaction.REPEATABLE;
        long xid = transactionManager.begin(level);
        for (Row row : rowList) {
            Long uid = table.insert(xid, row);
            uidList.add(uid);
        }

        // 开始一个新的事务，用新事务做查询
        long newXid = transactionManager.begin(level);
        Row row = table.select(newXid, new ValueInt(9, false));
        System.out.println(row);
        transactionManager.commit(newXid);


        // 提交第一个事务
        transactionManager.commit(xid);
        // 再开始一个新的事务
        long xid3 = transactionManager.begin(level);
        row = table.select(xid3, new ValueInt(9, false));
        System.out.println(row);
        transactionManager.commit(xid3);

    }

    @Test
    public void test() {
        System.out.println(197568495624L + "的页是："  + (197568495624L >>> 32));
        System.out.println(206158430216L + "的页是："  + (206158430216L >>> 32));
        System.out.println(201863463304L + "的页是："  + (201863463304L >>> 32));
    }
}
