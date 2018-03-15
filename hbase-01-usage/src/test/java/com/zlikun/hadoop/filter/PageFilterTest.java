package com.zlikun.hadoop.filter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * 分页过滤器
 *
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-02-28 17:26
 */
public class PageFilterTest {

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
    public void test() throws IOException {

        Scan scan = new Scan();
        PageFilter filter = new PageFilter(3);
        scan.setFilter(filter);
        scan.setStartRow(Bytes.toBytes("001384099200000000001284"));
        scan.setCaching(5);

        ResultScanner scanner = table.getScanner(scan);
        scanner.forEach(result -> {
            System.out.println(result);
        });

        scanner.close();

    }

}
