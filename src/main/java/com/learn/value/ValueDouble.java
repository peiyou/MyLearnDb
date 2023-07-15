package com.learn.value;

import java.nio.ByteBuffer;

/**
 * @author peiyou
 * @version 1.0
 * @className ValueDubble
 * @date 2023/7/11 22:00
 **/
public class ValueDouble extends Value {
    private final double value;

    private final boolean isNull;

    public ValueDouble() {
        this.value = 0.0;
        this.isNull = true;
    }
    public ValueDouble(ByteBuffer buffer, boolean isNull) {
        if (!isNull) {
            this.value = buffer.getDouble();
        } else {
            this.value = 0.0d;
        }
        this.isNull = isNull;
    }

    public ValueDouble(double value, boolean isNull) {
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
        if (this.isNull() && v.isNull()) {
            return -1;
        } else if (this.isNull()) {
            return -1;
        } else if (v.isNull()) {
            return 1;
        } else {
            double v2 = (double)v.getObject();
            if (this.value > 0 && v2 < 0) {
                return 1;
            } else if (v2 > 0 && this.value < 0) {
                return -1;
            } else {
                return (int)(this.value - v2);
            }
        }
    }

    @Override
    public byte[] getBytes() {
        if (isNull) {
            return new byte[0];
        }
        return ByteBuffer.allocate(Double.BYTES).putDouble(value).array();
    }

    @Override
    public int getType() {
        return Value.DOUBLE;
    }

    @Override
    public boolean isNull() {
        return isNull;
    }
}
