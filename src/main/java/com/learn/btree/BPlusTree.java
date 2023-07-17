package com.learn.btree;

import com.learn.data.DataItem;
import com.learn.data.DataManager;
import com.learn.page.Page;
import com.learn.page.PageCache;
import com.learn.table.Row;
import com.learn.value.Value;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * @author peiyou
 * @version 1.0
 * @className BPlusTree
 * @date 2023/7/14 14:46
 **/
public class BPlusTree {
    private Node root;
    // 最多有这么多个key
    private int maxKeys;
    // 除了根节点外，最少有多少个key
    private int minKeys;
    // 阶数
    private int m;

    private DataManager dataManager;

    private PageCache pageCache;

    public BPlusTree(DataManager dataManager, PageCache pageCache, long rootUid) throws Exception {
        /**
         * 按Node大小计算得去 409， 这里取400吧。 计算方式 看 Node类的注释。
         * {@link Node}
         */
        this.m = 400;
        this.maxKeys = m - 1;
        this.minKeys = (int)(Math.ceil(m / 2.0)) - 1;
        this.dataManager = dataManager;
        this.pageCache = pageCache;
        if (rootUid != 0) {
            root = loadNode(rootUid);
        }
    }

    public void add(Value key, long value) throws Exception {
        Entry entry = new Entry(key, value);
        if (root == null) {
            root = this.newNode();
            root.setDirty(true);
            root.addKey(entry);
            root.flush();
            return;
        }

        Value splitKey = add(root.getUid(), entry);
        if (splitKey != null) {
            // 更新根节点
            Node oldRoot = root;
            Node newRoot = this.newNode();
            newRoot.setDirty(true);
            newRoot.setLeaf(false);
            newRoot.getKeys().add(new Entry(splitKey, 0));
            newRoot.addChild(oldRoot.getUid());
            newRoot.addChild(oldRoot.getSibling());
            newRoot.flush();

            root = newRoot;
            // 更新表数据的根节点的uid
            Page rootPage = pageCache.get(1);
            rootPage.update(Page.DATA_OFFSET, ByteBuffer.allocate(Long.BYTES).putLong(root.getUid()).array());
            rootPage.release();
        }
    }



    /**
     * 添加值，如果返回一个非空的节点，说明发生了分裂
     * @param nodeUid
     * @param entry
     * @return
     */
    private Value add(long nodeUid, Entry entry) throws Exception {
        Node node = loadNode(nodeUid);
        List<Entry> keys = node.getKeys();
        int keyIndex = findKeyIndex(entry, keys);
        if (node.isLeaf()) {
            node.setDirty(true);
            // 查找插入位置
            if (keyIndex == keys.size()) {
                // 这种情况下，说明添加在末尾
                node.addKey(entry);
            } else {
                if (node.getKeys().get(keyIndex).getKey().compareTo(entry.getKey()) == 0) {
                    // 是同一条数据，更新
                    node.updateValue(keyIndex, entry.getValue());
                } else {
                    // 说明添加在中间
                    node.addKey(keyIndex, entry);
                }
            }
            node.flush();
            if (keys.size() > maxKeys) {
                return split(nodeUid);
            }
            return null;
        } else {
            // 是非叶子节点时
            Long nextUid = node.getChildList().get(keyIndex);
            node.flush();
            Value newKey = add(nodeUid, entry);
            if (newKey == null) {
                // 说明没有发生分裂了
                return null;
            } else {
                // 子节点发生了分裂
                // 获取到右节点的第一个key
                node.setDirty(true);
                node.addKey(new Entry(newKey, 0));
                Node next = loadNode(nextUid);
                node.addChild(next.getSibling());

                next.flush();
                node.flush();
                if (node.getKeys().size() > maxKeys) {
                    return split(nodeUid);
                }
            }
        }
        return null;
    }

    /**
     * 查找entry在keys中的位置，如果没找到可能是返回0和keys.size()
     * 返回 0 时 可能是找到了0号位置，判断key.compare(entity.getKey)==0。也可能是比第一个key还小。
     * 返回keys.size()时，肯定是比所有的key都大。
     * 返回中间位置时，可能是要插入数据的位置，也可能是数据本身。中间位置时，需要比较key的值与entity.getKey的值是否相等。
     * @author Peiyou
     * @date 2023/7/15 16:46
     * @param entry
     * @param keys
     * @return
     */
    private int findKeyIndex(Entry entry, List<Entry> keys) {
        int keyIndex = 0;
        while(keyIndex < keys.size()) {
            if (keys.get(keyIndex).compareTo(entry) <= 0) {
                keyIndex++;
            } else {
                break;
            }
        }
        return keyIndex;
    }

    /**
     * 分裂达到maxKeys的节点
     * 返回被分裂出来的key
     * @author Peiyou
     * @date 2023/7/14 17:28
     * @param nodeUid
     * @return
     */
    private Value split(long nodeUid) throws Exception {
        Node node = loadNode(nodeUid);
        Node newNode = this.newNode();
        node.setDirty(true);
        newNode.setDirty(true);
        newNode.setLeaf(node.isLeaf());

        int size = node.getKeys().size();
        int index = size / 2;
        int removeIndex = 0;

        Iterator<Entry> keyIterator = node.getKeys().iterator();
        // 处理key
        while (keyIterator.hasNext()) {
            Entry entry = keyIterator.next();
            if (removeIndex >= index) {
                keyIterator.remove();
                newNode.addKey(entry);
            }
            removeIndex++;
        }
        if (node.isLeaf()) {
            return newNode.getKeys().get(0).getKey();
        }
        // 处理子节点
        removeIndex = 0;
        Iterator<Long> iterator = node.getChildList().iterator();
        while (iterator.hasNext()) {
            Long childNode = iterator.next();
            // 请注意这里与上面的区别，少了个等号，因为对于子节点，是要多保留一个的。
            if (removeIndex > index) {
                iterator.remove();
                newNode.addChild(childNode);
            }
            removeIndex++;
        }
        Value key = newNode.getKeys().get(0).getKey();
        newNode.getKeys().remove(0);
        node.setSibling(newNode.getUid());
        newNode.flush();
        node.flush();
        return key;
    }

    public long search(Value key) throws Exception {
        Entry result = null;
        Entry entry = new Entry(key, 0);
        int index = Collections.binarySearch(root.getKeys(), entry);
        if (index < 0) {
            result = search(root.getUid(), entry);
        } else {
            if (root.isLeaf()) {
                result = root.getKeys().get(index);
            } else {
                Long node = root.getChildList().get(index + 1);
                result = search(node, entry);
            }
        }
        if (result == null) {
            return 0;
        }
        return result.getValue();
    }

    private Entry search(Long nodeUid, Entry entry) throws Exception {
        Node node = loadNode(nodeUid);
        node.flush();
        if (node.isLeaf()) {
            int index = Collections.binarySearch(node.getKeys(), entry);
            if (index < 0) {
                return null;
            } else {
                return node.getKeys().get(index);
            }
        } else {
            List<Entry> keys = node.getKeys();
            int keyIndex = findKeyIndex(entry, keys);
            long nextNode = node.getChildList().get(keyIndex);
            return search(nextNode, entry);
        }
    }

    public Node loadNode(long uid) throws Exception {
        DataItem dataItem = dataManager.select(uid);
        return new Node(dataItem, uid);
    }

    public static Node newNode(PageCache pageCache, DataManager dataManager) throws Exception {
        Page page = pageCache.newPage(Page.SIZE);
        int offset = page.write(DataItem.wrap(Node.initNodeData()));
        pageCache.releaseForCache(page);
        long uid = ((long)page.getPageNo()) << 32 | ((long)offset);
        DataItem dataItem = dataManager.select(uid);
        return new Node(dataItem, uid);
    }

    public Node newNode() throws Exception {
        return BPlusTree.newNode(this.pageCache, this.dataManager);
    }
}
