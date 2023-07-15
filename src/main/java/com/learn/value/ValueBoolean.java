package com.learn.value;

import java.nio.ByteBuffer;

/**
 * @author peiyou
 * @version 1.0
 * @className ValueBoolean
 * @date 2023/7/11 21:46
 **/
public class ValueBoolean extends Value {
    private final boolean value;

    private final boolean isNull;

    public ValueBoolean() {
        this.value = false;
        this.isNull = true;
    }
    public ValueBoolean(boolean value, boolean isNull) {
        this.value = value;
        this.isNull = isNull;
    }
    public ValueBoolean(ByteBuffer buffer, boolean isNull) {
        if (!isNull) {
            byte b = buffer.get();
            value = b == (byte)1;
        } else {
            value = false;
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
        return ByteBuffer.allocate(1).put(value ? (byte)1 : (byte)0).array();
    }

    @Override
    public int getType() {
        return Value.BOOLEAN;
    }

    @Override
    public boolean isNull() {
        return isNull;
    }
}
