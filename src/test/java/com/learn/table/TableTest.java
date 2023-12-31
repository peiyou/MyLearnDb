package com.learn.table;

import com.google.common.primitives.Bytes;
import com.learn.database.Database;
import com.learn.database.DatabaseManager;
import com.learn.transaction.Transaction;
import com.learn.transaction.TransactionManager;
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
 * @className TableTest
 * @date 2023/7/13 12:21
 **/
public class TableTest {


    private DatabaseManager databaseManager;

    private void init() {
        String path = "/Users/peiyou/myLearDb";
        databaseManager = new DatabaseManager(path);
    }
    @Test
    public void createDb() throws Exception {
        init();
        databaseManager.createDatabase("test");
    }

    private Table createTable(Database database, String tableName) throws Exception {
        database.dropTable(tableName);
        List<Column> columns = new ArrayList<>();
        columns.add(new Column("id", 0, Value.LONG, false, true));
        columns.add(new Column("name", 1, Value.STRING, true, false));
        columns.add(new Column("age", 2, Value.INT, true, false));
        columns.add(new Column("score", 3, Value.DOUBLE, true, false));
        columns.add(new Column("createDate", 4, Value.LONG, true, false));
        columns.add(new Column("createBy", 5, Value.LONG, false, false));
        return database.createTable(tableName, columns);
    }

    @Test
    public void insert() throws Exception {
        // 初始化
        init();
        String dbName = "test";
        String tableName = "test";
        Database database = databaseManager.getDatabase(dbName);
        Table table = this.createTable(database, tableName);

        List<Column> columns = new ArrayList<>();
        columns.add(new Column("id", 0, Value.LONG, false, true));
        columns.add(new Column("name", 1, Value.STRING, true, false));
        columns.add(new Column("age", 2, Value.INT, true, false));
        columns.add(new Column("score", 3, Value.DOUBLE, true, false));
        columns.add(new Column("createDate", 4, Value.LONG, true, false));
        columns.add(new Column("createBy", 5, Value.LONG, false, false));

        List<Row> rowList = new ArrayList<>();
        for (int i = 0; i < 10; i ++) {
            Value id = new ValueLong(i + 1, false);
            Value name = new ValueString("张三" + i);
            Value age = new ValueInt(18, false);
            Value score = new ValueDouble(60 + i, false);
            Value createDate = new ValueLong(new Date().getTime(), false);
            Value createBy = new ValueLong(1 + 100, false);
            byte[] rowBytes = Bytes.concat(id.getInputBytes(), name.getInputBytes(),
                    age.getInputBytes(), score.getInputBytes(), createDate.getInputBytes(),
                    createBy.getInputBytes());
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
        transactionManager.commit(xid);

        for (Long uid: uidList) {
            System.out.println(uid);
        }

        xid = transactionManager.begin(level);
        Row row = table.select(xid, new ValueInt(9, false));
        transactionManager.commit(xid);

        System.out.println(row);
    }
}
