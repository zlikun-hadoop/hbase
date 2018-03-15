package com.zlikun.hadoop.filter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-02-27 10:38
 */
public class CompareOpTest {

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
    public void GREATER_OR_EQUAL() throws IOException {

        Scan scan = new Scan();
        scan.setFilter(new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL,
                new BinaryComparator(Bytes.toBytes("001518019200000172000038"))));
        scan.setCaching(5);

        println(scan);

    }

    @Test
    public void GREATER() throws IOException {

        Scan scan = new Scan();
        scan.setFilter(new RowFilter(CompareFilter.CompareOp.GREATER,
                new BinaryComparator(Bytes.toBytes("001518019200000172000038"))));
        scan.setCaching(5);

        println(scan);

    }

    @Test
    public void LESS_OR_EQUAL() throws IOException {

        Scan scan = new Scan();
        scan.setFilter(new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL,
                new BinaryComparator(Bytes.toBytes("001384099200000000001284"))));
        scan.setCaching(5);

        println(scan);

    }

    @Test
    public void LESS() throws IOException {

        Scan scan = new Scan();
        scan.setFilter(new RowFilter(CompareFilter.CompareOp.LESS,
                new BinaryComparator(Bytes.toBytes("001384099200000000001284"))));
        scan.setCaching(5);

        println(scan);

    }

    @Test
    public void EQUAL() throws IOException {

        Scan scan = new Scan();
        scan.setFilter(new RowFilter(CompareFilter.CompareOp.EQUAL,
                new BinaryComparator(Bytes.toBytes("001384099200000000001284"))));
        scan.setCaching(5);

        println(scan);

    }

    @Test @Ignore // 数据量太大
    public void NOT_EQUAL() throws IOException {

        Scan scan = new Scan();
        scan.setFilter(new RowFilter(CompareFilter.CompareOp.NOT_EQUAL,
                new BinaryComparator(Bytes.toBytes("001384099200000000001284"))));
        scan.setCaching(5);

        println(scan);

    }

    /**
     * 排除一切值
     * @throws IOException
     */
    @Test
    public void NO_OP() throws IOException {

        Scan scan = new Scan();
        scan.setFilter(new RowFilter(CompareFilter.CompareOp.NO_OP,
                new BinaryComparator(Bytes.toBytes("001384099200000000001284"))));
        scan.setCaching(5);

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
