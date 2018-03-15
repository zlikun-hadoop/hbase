package com.zlikun.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018/2/5 21:11
 */
public class ScanTest {

    private Configuration configuration;
    private Connection connection;
    private Table table;

    @Before
    public void init() throws IOException {
        this.configuration = HBaseConfiguration.create();
        this.connection = ConnectionFactory.createConnection(configuration);
        this.table = connection.getTable(TableName.valueOf("user"));
    }

    @After
    public void destroy() throws IOException {
        connection.close();
        table.close();
    }

    /**
     * 每次获取数据都是一个RPC请求，对于小数据量时，性能较差
     *
     * @throws IOException
     */
    @Test
    public void scan() throws IOException {

        // 设定Scan对象，指定RowKey起始值(不包含结束值)
        Scan scan = new Scan(Bytes.toBytes("0001"), Bytes.toBytes("0004"))
                .addFamily(Bytes.toBytes("info"))  // 限定查询列族
                .addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"))   // 限定查询列名
                .addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"))   // 限定查询列名
//                .setTimeRange(1517493258482L, 1517495653164L)  // 查询时间戳范围
                .setMaxVersions(Integer.MAX_VALUE); // 查询每列版本数

        ResultScanner results = table.getScanner(scan);

        print(results);

    }

    @Test
    public void cache() throws IOException {

        // 设定Scan对象，指定RowKey起始值(不包含结束值)
        Scan scan = new Scan(Bytes.toBytes("0001"), Bytes.toBytes("0004"))
                .setCaching(10);    // 设置扫描缓存，可以一次获取指定数量条数据

        ResultScanner results = table.getScanner(scan);

        print(results);

    }

    @Test
    public void batch() throws IOException {

        Scan scan = new Scan(Bytes.toBytes("0001"), Bytes.toBytes("0004"))
                .setBatch(3);   // 缓存是面向行级操作、批量是面向列级操作，针对单行记录非常大时，如：文件，限定一次查询的列

        ResultScanner results = table.getScanner(scan);

        print(results);

    }

    private void print(ResultScanner results) throws IOException {
        try {
            Result result = null;
            // 一次获取一条记录
            while ((result = results.next()) != null) {
                int age = -1;
                if (result.containsColumn(Bytes.toBytes("info"), Bytes.toBytes("age"))) {
                    age = Bytes.toInt(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("age")));
                }
                System.out.println(
                        Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"))) + "," + age);
            }
        } finally {
            // 释放扫描资源(用完ResultScanner应及早关闭，减少资源占用)
            results.close();
        }
    }

}
