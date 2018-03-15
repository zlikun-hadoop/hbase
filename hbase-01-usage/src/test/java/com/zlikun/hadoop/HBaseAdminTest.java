package com.zlikun.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-02-27 16:26
 */
public class HBaseAdminTest {

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
        TableName name = TableName.valueOf("books");
//        admin.deleteTable(name);
        // 判断表是否存在
        assertFalse(admin.tableExists(name));
        // 创建表
        HTableDescriptor descriptor = new HTableDescriptor(name);
        descriptor.addFamily(new HColumnDescriptor("info"));
//        admin.createTable(descriptor);
        // 预分区建表[ descriptor, startKey, endKey, numRegions ]
        admin.createTable(descriptor, Bytes.toBytes(1), Bytes.toBytes(100), 5);
        // 增加、修改、删除字段
        admin.disableTable(name);
        admin.addColumn(name, new HColumnDescriptor("info2"));
        admin.modifyColumn(name, new HColumnDescriptor("info2"));
        admin.deleteColumn(name, Bytes.toBytes("info"));
        admin.enableTable(name);
        System.out.println(connection.getTable(name).getTableDescriptor());
        // 打印分区信息
        // 判断表是否存在
        assertTrue(admin.tableExists(name));
        // 检查表是否可用
        assertTrue(admin.isTableAvailable(name));
        // 修改表结构
        admin.disableTable(name);
        HTableDescriptor descriptor2 = new HTableDescriptor(name);
        descriptor2.addFamily(new HColumnDescriptor("info"));
        descriptor2.addFamily(new HColumnDescriptor("info2"));
        admin.modifyTable(name, descriptor2);
        admin.enableTable(name);
        System.out.println(connection.getTable(name).getTableDescriptor());
        // 禁用表
        admin.disableTable(name);
        // 删除表
        admin.deleteTable(name);
        admin.close();
    }

    @Test
    public void cluster() throws IOException {

        Admin admin = connection.getAdmin();
        ClusterStatus status = admin.getClusterStatus();

        System.out.println(status.getServersSize());
        status.getServers().forEach(System.out::println);
        System.out.println(status.getHBaseVersion());
        System.out.println(status.getDeadServers());
        // ... ...

        admin.close();
    }

}
