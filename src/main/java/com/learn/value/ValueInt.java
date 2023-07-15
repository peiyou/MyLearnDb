package com.learn.value;

import java.nio.ByteBuffer;

/**
 * @author peiyou
 * @version 1.0
 * @className ValueInt
 * @date 2023/7/11 20:39
 **/
public class ValueInt extends Value {

    private final int value;

    private final boolean isNull;

    public ValueInt() {
        this.value = 0;
        this.isNull = true;
    }
    public ValueInt(int value, boolean isNull) {
        this.value = value;
        this.isNull = isNull;
    }

    public ValueInt(ByteBuffer buffer, boolean isNull) {
        if (!isNull) {
            this.value = buffer.getInt();
        } else {
            this.value = 0;
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
        return ByteBuffer.allocate(Integer.BYTES).putInt(value).array();
    }

    @Override
    public int getType() {
        return Value.INT;
    }

    @Override
    public boolean isNull() {
        return isNull;
    }
}
