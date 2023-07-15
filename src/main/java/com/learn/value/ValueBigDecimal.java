package com.learn.value;

import com.google.common.primitives.Bytes;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

/**
 * @author peiyou
 * @version 1.0
 * @className ValueBigDecimal
 * @date 2023/7/12 10:49
 **/
public class ValueBigDecimal extends Value {
    private final BigDecimal value;

    private final boolean isNull;

    public ValueBigDecimal(BigDecimal value, boolean isNull) {
        this.value = value;
        this.isNull = isNull;
    }

    public ValueBigDecimal(ByteBuffer buffer, boolean isNull) {
        if (!isNull) {
            int size = buffer.getInt();
            byte[] bytes = new byte[size];
            buffer.get(bytes);
            value = new BigDecimal(new String(bytes));
        } else {
            value = null;
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
        String s = value.toPlainString();
        byte[] bytes = s.getBytes();
        byte[] size = ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array();
        return Bytes.concat(size, bytes);
    }

    @Override
    public int getType() {
        return Value.BIG_DECIMAL;
    }

    @Override
    public boolean isNull() {
        return isNull;
    }
}
