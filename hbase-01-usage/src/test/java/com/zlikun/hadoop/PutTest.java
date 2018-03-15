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

import static org.junit.Assert.assertEquals;

/**
 * PUT 命令测试
 *
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018/2/1 21:00
 */
public class PutTest {

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
    public void put() throws IOException {

        byte[] family = Bytes.toBytes("info");

        // 使用列族和时间戳构造Put实例
        Put put = new Put(Bytes.toBytes("0001"), System.currentTimeMillis());
        // 添加两列(Cell)，即：插入两条记录
        put.addColumn(family, Bytes.toBytes("name"), Bytes.toBytes("zlikun"));
        put.addColumn(family, Bytes.toBytes("age"), Bytes.toBytes(17));

        // 执行插入
        table.put(put);

        // 验证写入
        Result result = table.get(new Get(Bytes.toBytes("0001")));
        assertEquals("zlikun", Bytes.toString(result.getValue(family, Bytes.toBytes("name"))));
        assertEquals(17, Bytes.toInt(result.getValue(family, Bytes.toBytes("age"))));

    }

    @Test
    public void putList() throws IOException {

        final byte[] family = Bytes.toBytes("info");

        Put put = new Put(Bytes.toBytes("0001"), System.currentTimeMillis());
        put.addColumn(family, Bytes.toBytes("name"), Bytes.toBytes("zlikun"));
        put.addColumn(family, Bytes.toBytes("age"), Bytes.toBytes(17));

        Put put2 = new Put(Bytes.toBytes("0002"), System.currentTimeMillis());
        put2.addColumn(family, Bytes.toBytes("name"), Bytes.toBytes("Ashe"));
        put2.addColumn(family, Bytes.toBytes("gender"), Bytes.toBytes("female"));

        // 批量写入
        table.put(Arrays.asList(put, put2));

        // 可以在HBase服务上通过scan命令查看数据是否成功写入
        /*
        > scan 'user'
        ROW                     COLUMN+CELL
         0001                   column=info:age, timestamp=1517492549195, value=\x00\x00\x00\x11
         0001                   column=info:name, timestamp=1517492549195, value=zlikun
         0002                   column=info:gender, timestamp=1517492548930, value=female
         0002                   column=info:name, timestamp=1517492548930, value=Ashe
        2 row(s) in 0.0210 seconds
         */
    }

    @Test
    public void check_and_put() throws IOException {

        final byte[] family = Bytes.toBytes("info");

        // 使用列族和时间戳构造Put实例
        Put put = new Put(Bytes.toBytes("0001"), System.currentTimeMillis());
        // 添加两列(Cell)，即：插入两条记录
        put.addColumn(family, Bytes.toBytes("name"), Bytes.toBytes("zlikun"));
        put.addColumn(family, Bytes.toBytes("age"), Bytes.toBytes(17));

        // check-in-put，即：检查写，保证检查与写入的原子性，检查的数据存在时，才写入
        // 本例意为：当版本号为1时，执行写入，否则不写入
        table.checkAndPut(Bytes.toBytes("0001"), family, Bytes.toBytes("version"), Bytes.toBytes("1"), put);

        // 验证写入
        Result result = table.get(new Get(Bytes.toBytes("0001")));
        assertEquals("zlikun", Bytes.toString(result.getValue(family, Bytes.toBytes("name"))));
        assertEquals(24, Bytes.toInt(result.getValue(family, Bytes.toBytes("age"))));
    }

}
