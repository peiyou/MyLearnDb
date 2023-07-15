package com.learn.value;

import com.google.common.primitives.Bytes;
import com.learn.table.Column;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @author peiyou
 * @version 1.0
 * @className ValueTest
 * @date 2023/7/13 09:41
 **/
public class ValueTest {

    RandomAccessFile raf;
    FileChannel fileChannel;

    private void openFile() {
        String path = "/Users/peiyou/temp/test/test.db";
        new File(path).delete();
        File file = new File(path);
        try {
            file.createNewFile();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        if(!file.canRead() || !file.canWrite()) {
            throw new RuntimeException(path + "表不可读写.");
        }

        try {
            raf = new RandomAccessFile(file, "rw");
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        fileChannel = raf.getChannel();
    }

    @Test
    public void testInt() throws IOException {
        List<Column> columns = new ArrayList<>();
        Value[] values = new Value[10];
        for (int i=0; i < values.length; i++) {
            columns.add(new Column("name" + (i + 1), i, Value.INT, true, false));
            values[i] = new ValueInt(i + 1, false);
        }
        openFile();
        try {
            byte[] bytes = new byte[0];
            for (int i = 0; i < values.length; i++) {
                bytes = Bytes.concat(bytes, values[i].getInputBytes());
            }
            fileChannel.write(ByteBuffer.wrap(bytes));
            fileChannel.force(false);
        } catch (IOException e) {
            e.printStackTrace();
        }

        ByteBuffer buffer = null;
        long length = raf.length();
        System.out.println(length);
        buffer = ByteBuffer.allocate((int)length);
        fileChannel.position(0);
        fileChannel.read(buffer);

        Value[] readValues = new Value[10];
        buffer.position(0);
        for (int i =0; i < readValues.length; i++) {
            readValues[i] = Value.valueOf(buffer, columns.get(i));
            System.out.println(readValues[i].getObject());
        }
        raf.close();
        fileChannel.close();

        Assert.assertEquals(10, readValues[9].getObject());
    }

    @Test
    public void testIntAndNull() throws IOException {
        List<Column> columns = new ArrayList<>();
        Value[] values = new Value[10];
        for (int i=0; i < values.length; i++) {
            if (i % 4 == 0) {
                columns.add(new Column("name" + (i + 1), i, Value.INT, true, false));
                values[i] = new ValueInt(0, true);
            } else {
                columns.add(new Column("name" + (i + 1), i, Value.INT, true, false));
                values[i] = new ValueInt(i + 1, false);
            }
        }
        openFile();
        try {
            byte[] bytes = new byte[0];
            for (int i = 0; i < values.length; i++) {
                bytes = Bytes.concat(bytes, values[i].getInputBytes());
            }
            fileChannel.write(ByteBuffer.wrap(bytes));
            fileChannel.force(false);
        } catch (IOException e) {
            e.printStackTrace();
        }

        ByteBuffer buffer = null;
        long length = raf.length();
        System.out.println(length);
        buffer = ByteBuffer.allocate((int)length);
        fileChannel.position(0);
        fileChannel.read(buffer);

        Value[] readValues = new Value[10];
        buffer.position(0);
        for (int i =0; i < readValues.length; i++) {
            readValues[i] = Value.valueOf(buffer, columns.get(i));
            System.out.println(readValues[i].getObject());
        }
        raf.close();
        fileChannel.close();

        Assert.assertNull(readValues[8].getObject());
    }

    /**
     * 混合多种类型测试
     */
    @Test
    public void testMixture() throws IOException {
        List<Column> columns = new ArrayList<>();
        Value[] values = new Value[10];
        int[] types = this.getTypes();
        Random random = new Random();
        for (int i=0; i < values.length; i++) {
            int type = types[random.nextInt(7)];
//            int type = 1;
            System.out.println("type: " + type);
            if (i % 4 == 0) {
                columns.add(new Column("name" + (i + 1), i, type, true, false));
                values[i] = this.randomCreateType(type, true);
            } else {
                columns.add(new Column("name" + (i + 1), i, type, true, false));
                values[i] = this.randomCreateType(type, false);
            }
        }
        openFile();
        try {
            byte[] bytes = new byte[0];
            for (int i = 0; i < values.length; i++) {
                bytes = Bytes.concat(bytes, values[i].getInputBytes());
            }
            fileChannel.write(ByteBuffer.wrap(bytes));
            fileChannel.force(false);
        } catch (IOException e) {
            e.printStackTrace();
        }

        ByteBuffer buffer = null;
        long length = raf.length();
        System.out.println(length);
        buffer = ByteBuffer.allocate((int)length);
        fileChannel.position(0);
        fileChannel.read(buffer);

        Value[] readValues = new Value[10];
        buffer.position(0);
        System.out.println("========================");
        for (int i =0; i < readValues.length; i++) {
            readValues[i] = Value.valueOf(buffer, columns.get(i));
            System.out.println(readValues[i].getObject());
        }
        raf.close();
        fileChannel.close();
    }

    private int[] getTypes() {
        int[] types = new int[8];
        types[0] = Value.BYTES;
        types[1] = Value.BOOLEAN;
        types[2] = Value.SHORT;
        types[3] = Value.INT;
        types[4] = Value.LONG;
        types[5] = Value.DOUBLE;
        types[6] = Value.BIG_DECIMAL;
        types[7] = Value.STRING;
        return types;
    }

    private Value randomCreateType(int type, boolean isNull) {
        return switch (type) {
            case Value.BYTES -> {
                String value = """
                        使用switch时，如果遗漏了break，就会造成严重的逻辑错误，
                        而且不易在源代码中发现错误。从Java 12开始，switch语句升级为更简洁的表达式语法，
                        使用类似模式匹配（Pattern Matching）的方法，保证只有一种路径会被执行，并且不需要break语句：
                        """;
                byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
                yield new ValueBytes(bytes, isNull);
            }
            case Value.BOOLEAN -> new ValueBoolean(true, isNull);
            case Value.SHORT -> new ValueShort((short) 1, isNull);
            case Value.INT -> new ValueInt(3, isNull);
            case Value.LONG -> new ValueLong(Integer.MAX_VALUE, isNull);
            case Value.DOUBLE -> new ValueDouble(888.99, isNull);
            case Value.BIG_DECIMAL -> new ValueBigDecimal(new BigDecimal("99.333333333"), isNull);
            case Value.STRING -> new ValueString("""
                    从JEP 406 中的未来展望可以看出，此刻switch还不支持基本类型中的boolean，float，double，但是将来肯定会找到更合适的解决方案，让switch能支持全Java系列。
                    switch支持的解决方案越多，对于开发者来说能省掉很多的代码，间接的提高工作效率，提高程序的性能。期待未来的某一天switch的火力全开版本正式发布，那一天你还会看到 Java技术指北 的再一次更全面的解说。""");
            default -> throw new RuntimeException("不支持的类型");
        };
    }
}
