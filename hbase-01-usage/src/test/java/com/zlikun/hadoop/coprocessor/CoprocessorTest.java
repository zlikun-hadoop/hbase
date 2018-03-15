package com.zlikun.hadoop.coprocessor;


import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;

import java.io.IOException;

/**
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-02-27 13:53
 */
public class CoprocessorTest {

    public static class MyCoprocessor implements Coprocessor {

        @Override
        public void start(CoprocessorEnvironment env) throws IOException {
            System.out.println("==start==" + env.getHBaseVersion());
        }

        @Override
        public void stop(CoprocessorEnvironment env) throws IOException {
            System.out.println("==stop==" + env.getHBaseVersion());
        }
    }

}
