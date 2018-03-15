package com.zlikun.hadoop.counter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * > create 'counters', 'daily', 'weekly', 'monthly'
 *
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-02-27 13:27
 */
public class CounterTest {

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
    public void single() throws IOException {

        long counter = table.incrementColumnValue(Bytes.toBytes("20170101"), Bytes.toBytes("daily"), Bytes.toBytes("hits"), 1L);
        assertEquals(1L, counter);

        counter = table.incrementColumnValue(Bytes.toBytes("20170101"), Bytes.toBytes("daily"), Bytes.toBytes("hits"), 2L);
        assertEquals(3L, counter);

        counter = table.incrementColumnValue(Bytes.toBytes("20170101"), Bytes.toBytes("daily"), Bytes.toBytes("hits"), -1L);
        assertEquals(2L, counter);

        // 自增0，变相用于查询计数器值
        counter = table.incrementColumnValue(Bytes.toBytes("20170101"), Bytes.toBytes("daily"), Bytes.toBytes("hits"), 0L);
        assertEquals(2L, counter);

        // 重置计数器
        table.incrementColumnValue(Bytes.toBytes("20170101"), Bytes.toBytes("daily"), Bytes.toBytes("hits"), -2L);
    }

    @Test
    public void multi() throws IOException {

        Increment increment = new Increment(Bytes.toBytes("20170102"));
        increment.addColumn(Bytes.toBytes("daily"), Bytes.toBytes("hits"), 1L);
        increment.addColumn(Bytes.toBytes("weekly"), Bytes.toBytes("hits"), 2L);
        increment.addColumn(Bytes.toBytes("monthly"), Bytes.toBytes("hits"), 5L);
        Result result = table.increment(increment);
        System.out.println(result);

        long hits = Bytes.toLong(result.getValue(Bytes.toBytes("monthly"), Bytes.toBytes("hits")));
        System.out.println(hits);

    }

}
