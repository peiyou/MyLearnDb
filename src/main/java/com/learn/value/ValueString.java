package com.learn.value;

import com.google.common.primitives.Bytes;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * @author peiyou
 * @version 1.0
 * @className ValueString
 * @date 2023/7/11 22:04
 **/
public class ValueString extends Value {
    private final String value;

    private final boolean isNull;

    public ValueString() {
        this.value = null;
        this.isNull = true;
    }
    public ValueString(String value) {
        this.isNull = value == null;
        this.value = value;
    }

    public ValueString(ByteBuffer buffer) {
        int size = buffer.getInt();
        if (size == 0) {
            this.isNull = true;
            value = "";
        } else {
            this.isNull = false;
            byte[] bytes = new byte[size];
            buffer.get(bytes);
            this.value = new String(bytes, StandardCharsets.UTF_8);
        }
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
            return this.value.compareTo((String)v.getObject());
        }
    }

    @Override
    public byte[] getBytes() {
        if (isNull) {
            return new byte[0];
        }
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        byte[] size = ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array();
        return Bytes.concat(size, bytes);
    }

    @Override
    public int getType() {
        return Value.STRING;
    }

    @Override
    public boolean isNull() {
        return isNull;
    }
}
