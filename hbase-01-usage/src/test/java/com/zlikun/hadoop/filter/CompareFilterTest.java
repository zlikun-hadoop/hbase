package com.zlikun.hadoop.filter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-02-27 10:54
 */
public class CompareFilterTest {


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

    @Test
    public void testBinaryComparator() throws IOException {

        Scan scan = new Scan();
        scan.setFilter(new RowFilter(CompareFilter.CompareOp.EQUAL,
                new BinaryComparator(Bytes.toBytes("001384099200000000001284"))));
        scan.setCaching(5);

        println(scan);

    }

    @Test
    public void testBinaryPrefixComparator() throws IOException {

        Scan scan = new Scan();
        // 与BinaryComparator类似，但是是从左端开始前缀匹配
        scan.setFilter(new RowFilter(CompareFilter.CompareOp.EQUAL,
                new BinaryPrefixComparator(Bytes.toBytes("0013840992"))));
        scan.setCaching(5);

        println(scan);

    }

    @Test
    public void testNullComparator() throws IOException {

        Scan scan = new Scan();
        // 只判断当前值( 是指RowKey么？ )是否为NULL
        scan.setFilter(new RowFilter(CompareFilter.CompareOp.EQUAL,
                new NullComparator()));

        println(scan);

    }

    private void println(Scan scan) throws IOException {
        ResultScanner scanner = table.getScanner(scan);
        scanner.forEach(result -> {
            System.out.println(result);
        });

        scanner.close();
    }

}
