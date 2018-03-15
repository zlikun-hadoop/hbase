package com.zlikun.hadoop.filter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-02-27 10:54
 */
public class FamilyFilterTest {

    private Configuration configuration;
    private Connection connection;
    private Table table;

    @Before
    public void init() throws IOException {
        this.configuration = HBaseConfiguration.create();
        this.connection = ConnectionFactory.createConnection(configuration);
        this.table = connection.getTable(TableName.valueOf("certify"));
    }

    @After
    public void destroy() throws IOException {
        connection.close();
        table.close();
    }

    /**
     * 只返回指定行
     *
     * @throws IOException
     */
    @Test
    public void test() throws IOException {

        // 比较列族
        Filter filter = new FamilyFilter(CompareFilter.CompareOp.EQUAL,
                new BinaryComparator(Bytes.toBytes("i")));

        Get get = new Get(Bytes.toBytes("001384099200000000001284"));
        get.setFilter(filter);
        Result result = table.get(get);
        System.out.println(result);

    }

}
