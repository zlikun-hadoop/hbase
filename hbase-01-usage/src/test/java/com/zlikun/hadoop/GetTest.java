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
import java.util.Arrays;

import static org.junit.Assert.*;

/**
 * GET 命令测试
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018/2/01 21:55
 */
public class GetTest {

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

    @Test
    public void get() throws IOException {

        byte[] family = Bytes.toBytes("info");

        // 限定查询的行
        Get get = new Get(Bytes.toBytes("0001"));
        // 限定查询的列
        // get.addColumn(family, Bytes.toBytes("name"));

        // 一次获取一整行
        Result result = table.get(get);
        assertEquals("zlikun", Bytes.toString(result.getValue(family, Bytes.toBytes("name"))));
        assertEquals(24, Bytes.toInt(result.getValue(family, Bytes.toBytes("age"))));

    }

    @Test
    public void getList() throws IOException {

        // 批量查询
        Result [] results = table.get(Arrays.asList(new Get(Bytes.toBytes("0001")), new Get(Bytes.toBytes("0002"))));
        assertEquals(2, results.length);

    }

    @Test
    public void other() throws IOException {

        // 判断记录是否存在
        assertTrue(table.exists(new Get(Bytes.toBytes("0001"))));
        assertFalse(table.exists(new Get(Bytes.toBytes("0003"))));

    }

}
