package com.zlikun.hadoop.splitter;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RegionSplitter;

import java.math.BigInteger;

/**
 * 自定义分区算法，这里自动分[0000, 9999]区间，仅作演示
 * -----------------------------------------------------------------------------------------------
 $ HADOOP_CLASSPATH=`$HBASE_HOME/bin/hbase classpath`:/root/hadoop/hbase-02-splitter-1.0.0.jar \
 hbase org.apache.hadoop.hbase.util.RegionSplitter \
 -c 4 table_split_7 -f info \
 com.zlikun.hadoop.splitter.MySplitAlgorithm
 * -----------------------------------------------------------------------------------------------
 * @author zlikun <zlikun-dev@hotmail.com>
 * @date 2018-03-16 16:39
 */
public class MySplitAlgorithm implements RegionSplitter.SplitAlgorithm {

    BigInteger start = BigInteger.ZERO;
    BigInteger end = BigInteger.valueOf(10000);

    @Override
    public byte[] split(byte[] start, byte[] end) {
        return new byte[0];
    }

    /**
     * [~, 2500)
     * [2500, 5000)
     * [5000, 7500)
     * [7500, ~)
     * @param numRegions
     * @return
     */
    @Override
    public byte[][] split(int numRegions) {
        BigInteger range = end.subtract(start).add(BigInteger.ONE);
        BigInteger[] splits = new BigInteger[numRegions - 1];
        // 将区间划分成numRegions
        BigInteger sizeOfEachSplit = range.divide(BigInteger.valueOf(numRegions));
        for (int i = 1; i < numRegions; i++) {
            splits[i - 1] = start.add(sizeOfEachSplit.multiply(BigInteger.valueOf(i)));
        }
        return convertToBytes(splits);
    }

    private byte[][] convertToBytes(BigInteger[] splits) {
        byte[][] returnBytes = new byte[splits.length][];
        for (int i = 0; i < splits.length; i++) {
            returnBytes[i] = convertToByte(splits[i]);
        }
        return returnBytes;
    }

    private byte [] convertToByte(BigInteger split) {
        return Bytes.toBytes(split.toString());
    }

    @Override
    public byte[] firstRow() {
        return new byte[0];
    }

    @Override
    public byte[] lastRow() {
        return new byte[0];
    }

    @Override
    public void setFirstRow(String userInput) {

    }

    @Override
    public void setLastRow(String userInput) {

    }

    @Override
    public byte[] strToRow(String input) {
        return new byte[0];
    }

    @Override
    public String rowToStr(byte[] row) {
        return null;
    }

    @Override
    public String separator() {
        return null;
    }

    @Override
    public void setFirstRow(byte[] userInput) {

    }

    @Override
    public void setLastRow(byte[] userInput) {

    }
}
