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
import java.util.ArrayList;
import java.util.List;

/**
 * 批量操作，将一组操作打包执行
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018/2/01 22:12
 */
public class BatchTest {

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
    public void batch() throws IOException, InterruptedException {

        final byte[] family = Bytes.toBytes("info");

        List<Row> batch = new ArrayList<>();
        Put put = new Put(Bytes.toBytes("0004"), System.currentTimeMillis());
        put.addColumn(family, Bytes.toBytes("name"), Bytes.toBytes("Jane"));
        put.addColumn(family, Bytes.toBytes("age"), Bytes.toBytes(16));
        batch.add(put);

        Delete delete = new Delete(Bytes.toBytes("0001"));
        delete.addColumn(family, Bytes.toBytes("version"));
        batch.add(delete);

        Get get = new Get(Bytes.toBytes("0002"));
        batch.add(get);

        Result [] results = new Result[3] ;
        table.batch(batch, results);

        // keyvalues={0002/info:gender/1517492998626/Put/vlen=6/seqid=0, 0002/info:name/1517492998626/Put/vlen=4/seqid=0}
        System.out.println(results[2]);

    }

}
