package com.learn.table;

import com.google.common.primitives.Bytes;
import com.learn.page.Page;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * 创建表后，初始化数据
 *
 * @author peiyou
 * @version 1.0
 * @className Booter
 * @date 2023/7/12 17:02
 **/
public class Bootstrap {

    private static final String BOOT_SUFFIX = ".bt";

    /**
     * TODO 理论上应该先建个临时文件，然后到.bt文件，初始化表结构，最后把.bt文件重命名到.frm文件
     * TODO 防止中间过程中出现问题，导致无法再创建对应的表
     * 现在先直接创建最终的表
     * @author Peiyou
     * @date 2023/7/12 17:04
     * @param path
     * @param name
     * @return
     */
    public Bootstrap(String path, String name, List<Column> columns) {
        // 删除可能是坏的文件，也可能文件不存在，不过无所谓， 先删除一次。
        new File(path + File.separator + name + BOOT_SUFFIX).delete();

        File file = new File(path + File.separator + name + BOOT_SUFFIX);
        try {
            file.createNewFile();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if(!file.canRead() || !file.canWrite()) {
            throw new RuntimeException(name + "表不可读写.");
        }
        this.init(file, columns);
        file.renameTo(new File(path + File.separator + name + Table.frm));

        File idb = new File(path + File.separator + name + Table.idb);
        try {
            idb.createNewFile();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if(!idb.canRead() || !idb.canWrite()) {
            throw new RuntimeException(name + "表不可读写.");
        }
    }

    private void init(File file, List<Column> columns) {

        // 构造table的格式
        byte[] rootUidByte = ByteBuffer.allocate(Long.BYTES).putLong(0).array();
        byte[] sizeByte = ByteBuffer.allocate(Integer.BYTES).putInt(columns.size()).array();
        // 格式化列
        byte[] columnBytes = initData(columns);

        byte[] tableBytes = Bytes.concat(rootUidByte, sizeByte, columnBytes);
        int pageSize = ((tableBytes.length / Page.SIZE) + 1) * Page.SIZE;
        Page page = new Page(pageSize, new byte[pageSize], 1, null);
        page.write(tableBytes);
        try(FileOutputStream out = new FileOutputStream(file)) {
            out.write(page.getData());
            out.flush();
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

    private byte[] initData(List<Column> columns) {
        List<byte[]> result = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            result.add(columns.get(i).toByte());
        }
        byte[] bytes = result.get(0);
        for (int i = 1; i < result.size(); i++) {
            bytes = Bytes.concat(bytes, result.get(i));
        }
        return bytes;
    }
}
