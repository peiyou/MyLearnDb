package com.learn.data;

import com.google.common.primitives.Bytes;
import com.learn.page.Page;

/**
 * [valid][size][data]
 * 数据项，格式如上
 * @author peiyou
 * @version 1.0
 * @className DataItem
 * @date 2023/7/13 16:23
 **/
public class DataItem {
    private static final int VALID = 0;
    private static final int SIZE = VALID + 1;
    private static final int DATA = SIZE + 4;

    private byte[] raw;

    /**
     * 在页中的偏移量
     */
    private final int offset;

    // 对于更新时，用来保存旧值的
    private byte[] oldRaw;

    private Page page;

    public DataItem(byte[] raw, int offset, Page page) {
        this.raw = raw;
        this.offset = offset;
        this.page = page;
    }

    /**
     * 插入数据返回数据在页中的偏移量
     * @author Peiyou
     * @date 2023/7/14 09:12
     * @param data
     * @return int
     */
    public int insert(byte[] data) {
        byte[] wrap = wrap(data);
        return page.write(wrap);
    }

    /**
     * 更新有效性
     * @author Peiyou
     * @date 2023/7/14 09:22
     * @param valid
     * @return
     */
    public void updateValid(boolean valid) {
        oldRaw = raw;
        raw[0] = valid ? (byte)1 : (byte) 0;
        page.update(offset, raw);
    }

    public static byte[] wrap(byte[] data) {
        byte[] valid = new byte[]{(byte)1};
        byte[] size = new byte[4];
        return Bytes.concat(valid, size, data);
    }

    public boolean isValid() {
        return raw[0] == (byte)1;
    }

    public void release() throws Exception {
        page.release();
        raw = null;
        oldRaw = null;
        page = null;
    }

    public byte[] getData() {
        byte[] data = new byte[raw.length - DATA];
        System.arraycopy(this.raw, DATA, data, 0, data.length);
        return data;
    }
}
