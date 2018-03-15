package com.zlikun.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 准备测试数据
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018/2/5 21:56
 */
public class TestData {

    private Configuration configuration;
    private Connection connection;
    private Admin admin;
    private Table table;

    @Before
    public void init() throws IOException {
        this.configuration = HBaseConfiguration.create();
        this.connection = ConnectionFactory.createConnection(configuration);
        this.admin = this.connection.getAdmin();
        TableName tableName = TableName.valueOf("logs");
        this.table = connection.getTable(tableName);

        // 如果表不存在，则创建表
        createTable(tableName);
    }

    private void createTable(TableName tableName) throws IOException {
        if (admin.tableExists(tableName)) return;

        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
        tableDescriptor.addFamily(new HColumnDescriptor("info"));
        admin.createTable(tableDescriptor);
    }

    @After
    public void destroy() throws IOException {
        admin.close();
        table.close();
        connection.close();
    }

    @Test
    public void prepare() throws IOException {

        final byte[] family = Bytes.toBytes("info");

        int index = 0;
        for (int i = 0; i < 1000; i++) {
            List<Put> list = new ArrayList<>(100);
            for (int j = 0; j < 100; j++) {
                Put put = new Put(Bytes.toBytes(String.format("%010d", ++ index)));
                put.addColumn(family, Bytes.toBytes("number"), Bytes.toBytes(index));
                put.addColumn(family, Bytes.toBytes("message"), Bytes.toBytes("hello_" + index));
                list.add(put);
            }
            table.put(list);
        }

    }

}
