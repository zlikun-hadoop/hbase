package com.zlikun.hadoop.filter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018/2/5 22:06
 */
public class FilterTest {

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
    public void testRowFilter() throws IOException {

        Scan scan = new Scan();
        // 使用过滤器来获得特定行：过滤RowKey大于等于0000000100的记录
        // 使用过滤器时过滤操作将在服务器端生效，即：谓词下推
        // CompareFilter中的比较运算符：
        // LESS
        // LESS_OR_EQUAL
        // EQUAL
        // NOT_EQUAL
        // GREATER_OR_EQUAL
        // GREATER
        // NO_OP    排除一节值
        // HBase原生提供比较器：
        // BinaryComparator 使用Bytes.compareTo()比较当前值与阈值
        // BinaryPrefixComparator   与上面相似，使用Bytes.compareTo()进行匹配，但是是从左边开始前缀匹配
        // NullComparator   不做匹配，只判断当前值是否为空
        // BitComparator    通过BitwiseOp类提供的按位与、或、异或操作执行位级比较，只能与EQUAL或NOT_EQUAL运算符搭配使用
        // RegexStringComparator    根据一个正则表达式，在实例化这个比较器的时候去匹配表中的数据，只能与EQUAL或NOT_EQUAL运算符搭配使用
        // SubstringComparator  把阈值和表中数据当作String实例，同时通过contains()操作匹配字符串，只能与EQUAL或NOT_EQUAL运算符搭配使用
        // 比较过滤器
        // 行过滤器：RowFilter
        // 列族过滤器：FamilyFilter
        // 列名过滤器：QualifierFilter
        // 值过滤器：ValueFilter
        // 参考列过滤器：DependentColumnFilter
        // 专用过滤器
        // 单列值过滤器：SingleColumnValueFilter
        // 单列排除过滤器：SingleColumnValueExcludeFilter
        // 前缀过滤器：PrefixFilter
        // 分页过滤器：PageFilter
        // 行键过滤器：KeyOnlyFilter
        // 首次行键过滤器：FirstKeyOnlyFilter
        // 包含结束的过滤器：InclusiveStopFilter
        // 时间戳过滤器：TimestampFilter
        // 列计数过滤器：ColumnCountFilter
        // 列分页过滤器：ColumnPaginationFilter
        // 列前缀过滤器：ColumnPrefixFilter
        // 随机行过滤器：RandomRowFilter
        // 附加过滤器
        // 跳转过滤器：SkipFilter
        // 全匹配过滤器：WhileMatchFilter
        // FilterList
        // 自定义过滤器
        scan.setFilter(new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL,
                new BinaryComparator(Bytes.toBytes("001517932800000170753726"))));
        scan.setCaching(5);

        ResultScanner scanner = table.getScanner(scan);
        scanner.forEach(result -> {
            System.out.println(result);
        });

        scanner.close();
    }

    @Test
    public void testFamilyFilter() throws IOException {

        Scan scan = new Scan();
        scan.setFilter(new FamilyFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL,
                new BinaryComparator(Bytes.toBytes("userId"))));

        ResultScanner scanner = table.getScanner(scan);
        scanner.forEach(result -> {
            System.out.println(result);
        });
        scanner.close();

    }

}
