package com.zlikun.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * DELETE 命令测试
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018/2/01 22:12
 */
public class DeleteTest {

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
    public void delete() throws IOException {

        byte[] family = Bytes.toBytes("info");

        // 执行删除，单行记录删除(可以限定只删除指定列族或列)
        Delete delete = new Delete(Bytes.toBytes("0001"));
        delete.addColumn(family, Bytes.toBytes("version"));
        table.delete(delete);

    }

    @Test
    public void check_and_delete() throws IOException {

        byte[] family = Bytes.toBytes("info");
        Delete delete = new Delete(Bytes.toBytes("0001"));
        table.checkAndDelete(Bytes.toBytes("0001"), family, Bytes.toBytes("version"), Bytes.toBytes(1), delete);

    }

}
