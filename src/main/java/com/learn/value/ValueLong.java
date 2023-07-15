package com.learn.value;

import java.nio.ByteBuffer;

/**
 * @author peiyou
 * @version 1.0
 * @className ValueLong
 * @date 2023/7/11 21:56
 **/
public class ValueLong extends Value {

    private final long value;

    private final boolean isNull;

    public ValueLong() {
        this.value = 0L;
        this.isNull = true;
    }
    public ValueLong(long value, boolean isNull) {
        this.value = value;
        this.isNull = isNull;
    }

    public ValueLong(ByteBuffer buffer, boolean isNull) {
        if (!isNull) {
            this.value = buffer.getLong();
        }else {
            this.value = 0L;
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
        return ByteBuffer.allocate(Long.BYTES).putLong(value).array();
    }

    @Override
    public int getType() {
        return Value.LONG;
    }

    @Override
    public boolean isNull() {
        return isNull;
    }
}
