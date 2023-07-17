package com.learn.table;

import com.google.common.primitives.Bytes;
import com.learn.value.Value;
import com.learn.value.ValueDouble;
import com.learn.value.ValueInt;
import com.learn.value.ValueLong;
import com.learn.value.ValueString;
import org.junit.Test;

import java.io.File;
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

    @Test
    public void create() throws Exception {
        String path = "/Users/peiyou/temp/test";
        String tableName = "test";
        this.create();
    }

    private Table create(String path, String tableName) throws Exception {
        delete(path, tableName);
        List<Column> columns = new ArrayList<>();
        columns.add(new Column("id", 0, Value.LONG, false, true));
        columns.add(new Column("name", 1, Value.STRING, true, false));
        columns.add(new Column("age", 2, Value.INT, true, false));
        columns.add(new Column("score", 3, Value.DOUBLE, true, false));
        columns.add(new Column("createDate", 4, Value.LONG, true, false));
        columns.add(new Column("createBy", 5, Value.LONG, false, false));

        Table test = Table.create(path, tableName, columns);
        System.out.println(test);
        return test;
    }

    private void delete(String path, String tableName) {
        new File(path + File.separator + tableName + Table.frm).delete();
        new File(path + File.separator + tableName + Table.idb).delete();
    }

    @Test
    public void insert() throws Exception {
        String path = "/Users/peiyou/temp/test";
        String tableName = "test";
        Table table = this.create(path, tableName);

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
        for (Row row : rowList) {
            long uid = table.insert(row);
            uidList.add(uid);
        }

        for (Long uid: uidList) {
            System.out.println(uid);
        }

        Row row = table.select(new ValueInt(9, false));
        System.out.println(row);
    }
}
