package com.zlikun.hadoop.benchmark;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

/**
 * 性能测试：写入数据
 *
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-03-16 09:38
 */
@Slf4j
public class PutTest {

    private Configuration configuration;
    private Connection connection;
    private Table table;
    private long time;

    @Before
    public void init() throws IOException {
        // 准备表
        this.configuration = HBaseConfiguration.create();
        this.connection = ConnectionFactory.createConnection(configuration);
        TableName tableName = TableName.valueOf("benchmark");
        this.table = connection.getTable(tableName);

        // 检查并创建该表
        Admin admin = connection.getAdmin();
        if (!admin.tableExists(tableName)) {
            log.info("表不存在，执行建表 ...");
            HTableDescriptor descriptor = new HTableDescriptor(tableName);
            HColumnDescriptor columnDescriptor = new HColumnDescriptor("info");
            descriptor.addFamily(columnDescriptor);
            admin.createTable(descriptor);
        }
        admin.close();

        // 开始计时
        time = System.currentTimeMillis();
    }

    @After
    public void destroy() throws IOException {
        System.out.printf("程序执行耗时：%d 毫秒！\n", System.currentTimeMillis() - time);
        connection.close();
        table.close();
    }

    /**
     * 单线程、单条写入，100万条，耗时 1,022,426 毫秒，平均写入速度：978 t/s
     * @throws IOException
     */
    @Test @Ignore
    public void put_single() throws IOException {
        log.info("开始写入数据 ...");
        final byte[] family = Bytes.toBytes("info");
        // 写入100万条记录，每条记录有1个列族，5个列
        for (int i = 0; i < 1_000_000; i++) {
            Put put = new Put(Bytes.toBytes(String.format("%08d", i)));
            put.addColumn(family, Bytes.toBytes("field_1_" + i), Bytes.toBytes("field_1_value_" + i));
            put.addColumn(family, Bytes.toBytes("field_2_" + i), Bytes.toBytes("field_2_value_" + i));
            put.addColumn(family, Bytes.toBytes("field_3_" + i), Bytes.toBytes("field_3_value_" + i));
            put.addColumn(family, Bytes.toBytes("field_4_" + i), Bytes.toBytes("field_4_value_" + i));
            put.addColumn(family, Bytes.toBytes("field_5_" + i), Bytes.toBytes(System.currentTimeMillis()));
            table.put(put);
        }
    }

    /**
     * 并行写入，这里使用ParallelStream实现并发(也可以使用线程池)
     * 并发、单条写入，100万条，耗时 827,803 毫秒，平均写入速度：1207 t/s
     * @throws IOException
     */
    @Test @Ignore
    public void put_multi() throws IOException {
        log.info("开始写入数据 ...");
        final byte[] family = Bytes.toBytes("info");
        // 写入100万条记录，每条记录有1个列族，5个列
        IntStream.range(1_000_000, 2_000_000)
                .parallel()
                .forEach(i -> {
                    Put put = new Put(Bytes.toBytes(String.format("%08d", i)));
                    put.addColumn(family, Bytes.toBytes("field_1_" + i), Bytes.toBytes("field_1_value_" + i));
                    put.addColumn(family, Bytes.toBytes("field_2_" + i), Bytes.toBytes("field_2_value_" + i));
                    put.addColumn(family, Bytes.toBytes("field_3_" + i), Bytes.toBytes("field_3_value_" + i));
                    put.addColumn(family, Bytes.toBytes("field_4_" + i), Bytes.toBytes("field_4_value_" + i));
                    put.addColumn(family, Bytes.toBytes("field_5_" + i), Bytes.toBytes(System.currentTimeMillis()));
                    try {
                        table.put(put);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
    }

    /**
     * 并行写入，这里使用ParallelStream实现并发(也可以使用线程池)
     * 并发、批量写入，100万条，耗时 62,564 毫秒，平均写入速度：15873 t/s
     * @throws IOException
     */
    @Test @Ignore
    public void put_multi_batch() throws IOException {
        log.info("开始写入数据 ...");
        final byte[] family = Bytes.toBytes("info");
        // 写入100万条记录，每条记录有1个列族，5个列
        // 区间值缩小100倍，内部批量一次100条数据
        IntStream.range(20_000, 30_000)
                .parallel()
                .forEach(i -> {
                    // 一次写入100条
                    List<Put> list = new ArrayList<>();
                    for (int j = 0; j < 100; j++) {
                        int number = i * 100 + j;
                        Put put = new Put(Bytes.toBytes(String.format("%08d", number)));
                        put.addColumn(family, Bytes.toBytes("field_1_" + number), Bytes.toBytes("field_1_value_" + number));
                        put.addColumn(family, Bytes.toBytes("field_2_" + number), Bytes.toBytes("field_2_value_" + number));
                        put.addColumn(family, Bytes.toBytes("field_3_" + number), Bytes.toBytes("field_3_value_" + number));
                        put.addColumn(family, Bytes.toBytes("field_4_" + number), Bytes.toBytes("field_4_value_" + number));
                        put.addColumn(family, Bytes.toBytes("field_5_" + number), Bytes.toBytes(System.currentTimeMillis()));
                        list.add(put);
                    }
                    try {
                        table.put(list);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
    }

}
