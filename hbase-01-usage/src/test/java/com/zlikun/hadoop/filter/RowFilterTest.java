package com.zlikun.hadoop.filter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-02-27 10:54
 */
public class RowFilterTest {

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

        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("i"), Bytes.toBytes("userId"));
        scan.addColumn(Bytes.toBytes("i"), Bytes.toBytes("schoolId"));
        scan.setFilter(new RowFilter(CompareFilter.CompareOp.LESS,
                new BinaryComparator(Bytes.toBytes("001384099200000000001284"))));
        scan.setCaching(5);

        println(scan, 1);

        scan.setFilter(new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("^\\d+2\\d0284$")));
        println(scan, 2);

    }

    private void println(Scan scan, int index) throws IOException {
        System.out.println("------------------" + index);
        ResultScanner scanner = table.getScanner(scan);
        scanner.forEach(result -> {
            System.out.println(result);
        });

        scanner.close();
    }

}
