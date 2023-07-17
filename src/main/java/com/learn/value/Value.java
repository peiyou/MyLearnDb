package com.learn.value;

import com.google.common.primitives.Bytes;
import com.learn.table.Column;

import java.nio.ByteBuffer;

/**
 * @author peiyou
 * @version 1.0
 * @className Value
 * @date 2023/7/11 20:24
 **/
public abstract class Value implements Comparable<Value> {

    public static final int NULL = -1;
    public static final int BYTES = 1;
    public static final int BOOLEAN = 2;
    public static final int SHORT = 3;
    public static final int INT = 4;
    public static final int LONG = 5;
    public static final int DOUBLE = 6;
    public static final int BIG_DECIMAL = 7;
    public static final int STRING = 8;

    /**
     * 获取真实的值
     * @author Peiyou
     * @date 2023/7/11 20:26
     * @return
     */
    public abstract Object getObject();

    /**
     * 比较器, 用于排序
     * @author Peiyou
     * @date 2023/7/11 20:27
     * @param v
     * @return
     */
    public abstract int compareTo(Value v);

    /**
     * 数据类的比较
     * @author Peiyou
     * @date 2023/7/11 21:41
     * @param o1
     * @param o2
     * @return
     */
    protected static int numberCompare(Value o1, Value o2) {
        if (o1.isNull() && o2.isNull()) {
            return -1;
        } else if (o1.isNull()) {
            return -1;
        } else if (o2.isNull()) {
            return 1;
        } else {
            long v1 = Long.parseLong(o1.getObject().toString());
            long v2 = Long.parseLong(o2.getObject().toString());
            if (v1 > 0 && v2 < 0) {
                return 1;
            } else if (v2 > 0 && v1 < 0) {
                return -1;
            } else {
                return (int)(v1 - v2);
            }
        }
    }

    /**
     * 获取到对应格式的字节数组
     * @author Peiyou
     * @date 2023/7/11 20:49
     * @return
     */
    public abstract byte[] getBytes();

    /**
     * 获取可直接插入文件的bytes
     * [type][data]
     * @author Peiyou
     * @date 2023/7/13 09:37
     * @return byte[]
     */
    public byte[] getInputBytes() {
        byte[] type;
        if (!isNull()) {
            type = ByteBuffer.allocate(Integer.BYTES).putInt(this.getType()).array();
        } else {
            type = ByteBuffer.allocate(Integer.BYTES).putInt(Value.NULL).array();
        }
        return Bytes.concat(type, this.getBytes());
    }

    /**
     * 获取到类型
     * @author Peiyou
     * @date 2023/7/11 20:52
     * @return
     */
    public abstract int getType();

    /**
     * 是空值
     * @author Peiyou
     * @date 2023/7/11 21:00
     * @return
     */
    public abstract boolean isNull();

    public static Value valueOf(ByteBuffer buffer, Column column) {
        int type = buffer.getInt();
        return switch (type) {
            case Value.NULL -> {
                int columnType = column.type();
                yield Value.getValueByType(columnType, buffer, true);
            }
            default -> Value.getValueByType(type, buffer, false);
        };
    }

    public static Value getValueByType(int type, ByteBuffer buffer, boolean isNull) {
        return switch (type) {
            case Value.BYTES -> new ValueBytes(buffer, isNull);
            case Value.BOOLEAN -> new ValueBoolean(buffer, isNull);
            case Value.SHORT -> new ValueShort(buffer, isNull);
            case Value.INT -> new ValueInt(buffer, isNull);
            case Value.LONG -> new ValueLong(buffer, isNull);
            case Value.DOUBLE -> new ValueDouble(buffer, isNull);
            case Value.BIG_DECIMAL -> new ValueBigDecimal(buffer, isNull);
            case Value.STRING -> new ValueString(buffer);
            default -> throw new RuntimeException("不支持的类型");
        };
    }
}
