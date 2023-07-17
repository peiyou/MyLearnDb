package com.learn.btree;

import com.google.common.primitives.Bytes;
import com.learn.data.DataItem;
import com.learn.page.Page;
import com.learn.value.Value;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * [isLeaf][keySize][sibling]
 * [key0][key1]...[keyN]
 * [child0][child1]...[childN+1]
 *
 * 非叶子节点的child会比keySize多一个。
 * 叶子节点的child刚好就是记录对应的uid。就不会多一个。
 *
 * 先考虑key最大支持 32字节（相当于最多可以放的实际值是 32 - 8 = 24个字节大小，因为value类型中type占4字节,size也占4字节,不为数字的类型都会按string存）
 * 所以 基本大小  1 + 4 + 8 (isLeaf + keySize + sibling) 的大小  13B
 * key 按最大32字节算
 * child 占8字节
 * 一个16KB的页，最多可以放
 * 第一个13是 (isLeaf + keySize + sibling)的大小，第二个13是DataItem 中浪费的 加 上Page 中浪费的 (valid, size, pageSize, pageOffset)
 * 需要有 n个key和n+1个child 所以最多可以放 n * 32 + (n + 1) * 8 = 16 * 1024 - 13 - 13 取个整数 400个。 会浪费一点点空间，就算这样吧。
 *
 *
 * @author peiyou
 * @version 1.0
 * @className Node
 * @date 2023/7/14 14:46
 **/
public class Node {
    // 节点的uid
    private long uid;
    private List<Entry> keys;
    private List<Long> childList;

    private long sibling;
    private boolean isLeaf;

    private DataItem dataItem;

    public static final int NODE_SIZE = Page.SIZE - Page.DATA_OFFSET - DataItem.DATA;

    private boolean dirty;

    private Node() {
        keys = new ArrayList<>();
        childList = new ArrayList<>();
        sibling = 0;
        isLeaf = false;
    }

    public Node(DataItem dataItem, long uid) {
        this.dataItem = dataItem;
        keys = new ArrayList<>();
        childList = new ArrayList<>();
        ByteBuffer buffer = ByteBuffer.wrap(dataItem.getData());
        byte leaf = buffer.get();
        this.isLeaf = leaf == (byte)1;
        this.uid  = uid;
        int keySize = buffer.getInt();
        this.sibling = buffer.getLong();
        Value[] keyArray = new Value[keySize];
        for (int i = 0; i < keySize; i ++) {
            // 主键肯定不能为空, 所以type一定是具体的值
            int type = buffer.getInt();
            Value key = Value.getValueByType(type, buffer, false);
            keyArray[i] = key;
        }
        // 读取子节点，有两种可能
        // 非叶子节点
        if (!isLeaf) {
            for (int i = 0; i < keySize + 1; i++) {
                childList.add(buffer.getLong());
            }
            for (Value key: keyArray) {
                keys.add(new Entry(key, 0));
            }
        } else {
            for (int i = 0; i < keySize; i++) {
                long value = buffer.getLong();
                keys.add(new Entry(keyArray[i], value));
            }
        }
    }

    public List<Entry> getKeys() {
        return keys;
    }

    public void setKeys(List<Entry> keys) {
        this.keys = keys;
    }

    public List<Long> getChildList() {
        return childList;
    }

    public void setChildList(List<Long> childList) {
        this.childList = childList;
    }

    public long getSibling() {
        return sibling;
    }

    public void setSibling(long sibling) {
        this.sibling = sibling;
    }

    public boolean isLeaf() {
        return isLeaf;
    }

    public void setLeaf(boolean leaf) {
        isLeaf = leaf;
    }

    public long getUid() {
        return uid;
    }

    public void setUid(long uid) {
        this.uid = uid;
    }

    public void addKey(Entry entry) {
        this.getKeys().add(entry);
    }

    public void addKey(int index, Entry entry) {
        this.getKeys().add(index, entry);
    }
    public void addChild(Long node) {
        this.getChildList().add(node);
    }

    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    /**
     * 只更新key对应的实体类的value值
     * @author Peiyou
     * @date 2023/7/15 16:35
     * @param index
     * @param value
     * @return
     */
    public void updateValue(int index, long value) {
        this.getKeys().get(index).setValue(value);
    }

    public static byte[] initNodeData() {
        byte[] nodeData = new byte[Node.NODE_SIZE];
        nodeData[0] = (byte) 1; // isLeaf
        byte[] keySize = ByteBuffer.allocate(Integer.BYTES).putInt(0).array();
        System.arraycopy(keySize, 0, nodeData, 1, keySize.length);
        byte[] sibling = ByteBuffer.allocate(Long.BYTES).putLong(0).array();
        System.arraycopy(sibling, 0, nodeData, 5, sibling.length);
        return nodeData;
    }

    public void flush() throws Exception {
        if (dirty) {
            this.reWriteData();
        }
        this.dataItem.force();
        this.dirty = false;
    }

    /**
     * 更改node对象后，将对象数据回写到文件中
     */
    private void reWriteData() {
        byte[] isLeaf = new byte[] {this.isLeaf ? (byte) 1 : (byte) 0};
        byte[] keySize = ByteBuffer.allocate(Integer.BYTES).putInt(this.keys.size()).array();
        byte[] sibling = ByteBuffer.allocate(Long.BYTES).putLong(this.sibling).array();
        byte[] keyBytes = new byte[0];
        for (int i = 0; i < this.keys.size(); i++) {
            keyBytes = Bytes.concat(keyBytes, this.keys.get(i).getKey().getInputBytes());
        }
        // child
        byte[] childBytes = new byte[0];
        if (this.isLeaf) {
            for (int i = 0; i < this.keys.size(); i++) {
               long value = this.keys.get(i).getValue();
                childBytes = Bytes.concat(childBytes, ByteBuffer.allocate(Long.BYTES).putLong(value).array());
            }
        } else {
            for (int i = 0; i < this.childList.size(); i++) {
                long childUid = this.childList.get(i);
                childBytes = Bytes.concat(childBytes, ByteBuffer.allocate(Long.BYTES).putLong(childUid).array());
            }
        }
        this.dataItem.update(Bytes.concat(isLeaf, keySize, sibling, keyBytes, childBytes));
    }
}
