package com.learn.btree;


import com.learn.value.Value;

/**
 * key是主键值
 * value对应记录的uid，所以直接写long类型就够了。
 * @author peiyou
 * @version 1.0
 * @className Entry
 * @date 2023/7/14 15:39
 **/
public class Entry implements Comparable<Entry>{
    private Value key;
    private long value;

    public Entry(Value key, long value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public int compareTo(Entry o) {
        return this.key.compareTo(o.key);
    }

    public Value getKey() {
        return key;
    }

    public void setKey(Value key) {
        this.key = key;
    }

    public long getValue() {
        return value;
    }

    public void setValue(long value) {
        this.value = value;
    }
}
