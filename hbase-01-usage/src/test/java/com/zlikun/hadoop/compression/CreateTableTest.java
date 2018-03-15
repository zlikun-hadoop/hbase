package com.zlikun.hadoop.compression;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-03-15 18:12
 */
public class CreateTableTest {

    private Configuration configuration;
    private Connection connection;
    private Table table;

    @Before
    public void init() throws IOException {
        this.configuration = HBaseConfiguration.create();
        this.connection = ConnectionFactory.createConnection(configuration);
        this.table = connection.getTable(TableName.valueOf("counters"));
    }

    @After
    public void destroy() throws IOException {
        connection.close();
        table.close();
    }

    @Test
    public void admin() throws IOException {
        Admin admin = connection.getAdmin();
        TableName name = TableName.valueOf("servers");

        // 表定义
        HTableDescriptor descriptor = new HTableDescriptor(name);

        // 创建列族
        HColumnDescriptor columnDescriptor = new HColumnDescriptor("info");
        // 列族开启压缩
        columnDescriptor.setCompressionType(Compression.Algorithm.GZ);
        // 增加列族
        descriptor.addFamily(columnDescriptor);

        // 创建表
        admin.createTable(descriptor);

        // 禁用表
        admin.disableTable(name);

        // 删除表
        admin.deleteTable(name);

        admin.close();
    }

}
