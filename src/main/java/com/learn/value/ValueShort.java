package com.learn.value;

import java.nio.ByteBuffer;

/**
 * @author peiyou
 * @version 1.0
 * @className ValueShort
 * @date 2023/7/11 21:38
 **/
public class ValueShort extends Value {

    private final short value;

    private final boolean isNull;

    public ValueShort() {
        this.value = (short) 0;
        this.isNull = true;
    }
    public ValueShort(ByteBuffer buffer, boolean isNull) {
        if (!isNull) {
            this.value = buffer.getShort();
        } else {
            this.value = 0;
        }
        this.isNull = isNull;
    }

    public ValueShort(short value, boolean isNull) {
        this.value = value;
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
        return ByteBuffer.allocate(Short.BYTES).putShort(value).array();
    }

    @Override
    public int getType() {
        return Value.SHORT;
    }

    @Override
    public boolean isNull() {
        return isNull;
    }
}
