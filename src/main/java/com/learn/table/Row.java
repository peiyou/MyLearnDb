package com.learn.table;

import com.google.common.primitives.Bytes;
import com.learn.value.Value;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * @author peiyou
 * @version 1.0
 * @className Row
 * @date 2023/7/12 11:12
 **/
public class Row {

    private Value[] columns;

    /**
     *
     * @param buffer 一条数据
     * @param columnNames 对应地字段
     */
    public Row(ByteBuffer buffer, List<Column> columnNames) {
        columns = new Value[columnNames.size()];
        for (Column column: columnNames) {
            columns[column.index()] = Value.valueOf(buffer, column);
        }
    }

    public Row(Value[] columns) {
        this.columns = columns;
    }

    public Value get(int index) {
        if (index <0 || index >= columns.length) {
            throw new RuntimeException("get 数据不存在");
        }
        return columns[index];
    }

    public byte[] getBytes() {
        byte[] result = new byte[0];
        for (Value value: columns) {
            result = Bytes.concat(result, value.getInputBytes());
        }
        return result;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Value value: columns) {
            sb.append(value.getObject()).append(", ");
        }
        return sb.toString();
    }

    public void setCol(int index, Value value) {
        columns[index] = value;
    }
}
