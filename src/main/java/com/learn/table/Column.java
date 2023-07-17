package com.learn.table;

import com.google.common.primitives.Bytes;

import java.nio.ByteBuffer;

/**
 * [name][index][type][isNull][isPrimaryKey]
 * 每个字段在文件中存放的信息
 * name字段由 [size][data]组成
 *
 * table中的列，对应字段
 * @author peiyou
 * @version 1.0
 * @className Column
 * @date 2023/7/12 11:13
 **/
public record Column(String name, int index, int type, boolean isNull, boolean isPrimaryKey) {

    public static Column instance(ByteBuffer buffer) {
        String name = Column.getName(buffer);
        int index = buffer.getInt();
        int type = buffer.getInt();
        boolean isNull = getBoolean(buffer);
        boolean isPrimaryKey = getBoolean(buffer);
        return new Column(name, index, type, isNull, isPrimaryKey);
    }

    private static String getName(ByteBuffer buffer) {
        int size = buffer.getInt();
        byte[] bytes = new byte[size];
        buffer.get(bytes);
        return new String(bytes);
    }

    private static boolean getBoolean(ByteBuffer buffer) {
        return buffer.get() == (byte) 1;
    }

    public byte[] toByte() {
        byte[] nameByte = this.name.getBytes();
        int nameSize = nameByte.length;
        /**
         * nameSize 占4字节
         * nameByte 占nameByte.length字节
         * index 占4字节
         * type 占4字节
         * isNull 占1字节
         * isPrimaryKey 占1字节
         */
        byte[] nameSizeByte = ByteBuffer.allocate(Integer.BYTES).putInt(nameSize).array();
        byte[] indexByte = ByteBuffer.allocate(Integer.BYTES).putInt(this.index).array();
        byte[] typeByte = ByteBuffer.allocate(Integer.BYTES).putInt(this.type).array();
        byte[] isNullByte = ByteBuffer.allocate(1).put(this.isNull ? (byte)1 : (byte)0).array();
        byte[] isPrimaryKeyByte = ByteBuffer.allocate(1).put(this.isPrimaryKey ? (byte)1: (byte)0).array();
        return Bytes.concat(nameSizeByte, nameByte, indexByte, typeByte, isNullByte, isPrimaryKeyByte);
    }
}
