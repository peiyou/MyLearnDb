package com.learn.value;

import com.google.common.primitives.Bytes;

import java.nio.ByteBuffer;

/**
 * @author peiyou
 * @version 1.0
 * @className ValueBytes
 * @date 2023/7/11 21:16
 **/
public class ValueBytes extends Value {

    private final byte[] value;
    private final boolean isNull;

    public ValueBytes() {
        this.value = new byte[0];
        this.isNull = true;
    }
    public ValueBytes(byte[] value, boolean isNull) {
        this.value = value;
        this.isNull = isNull;
    }

    public ValueBytes(ByteBuffer buffer, boolean isNull) {
        if (isNull) {
            value = new byte[0];
        } else {
            int size = buffer.getInt();
            // 这就决定了最大可以放4GB = 2的32次方字节
            value = new byte[size];
            buffer.get(value);
        }
        this.isNull = isNull;
    }

    @Override
    public Object getObject() {
        if (isNull) return null;
        return value;
    }

    @Override
    public int compareTo(Value v) {
        return Value.numberCompare(this, v);
    }

    @Override
    public byte[] getBytes() {
        if (isNull) {
            return new byte[0];
        }
        byte[] size = ByteBuffer.allocate(Integer.BYTES).putInt(value.length).array();
        return Bytes.concat(size, value);
    }

    @Override
    public int getType() {
        return Value.BYTES;
    }

    @Override
    public boolean isNull() {
        return isNull;
    }
}
