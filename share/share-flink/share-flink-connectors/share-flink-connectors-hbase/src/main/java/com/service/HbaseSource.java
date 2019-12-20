package com.service;

import org.apache.flink.addons.hbase.TableInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseSource extends TableInputFormat<Tuple2<String, String>> {
    private Tuple2<String, String> reuse = new Tuple2<String, String>();

    public static final String HBASE_TABLE_NAME = "zhisheng";
    // 列族
    static final byte[] INFO = "info".getBytes();
    //列名
    static final byte[] BAR = "bar".getBytes();
    protected Scan getScanner() {
        Scan scan = new Scan();
        scan.addColumn(INFO, BAR);
        return scan;
    }

    protected String getTableName() {
        return "";
    }

    protected Tuple2<String, String> mapResultToTuple(Result result) {
        String key = Bytes.toString(result.getRow());
        String val = Bytes.toString(result.getValue(INFO, BAR));
        reuse.setField(key, 0);
        reuse.setField(val, 1);
        return reuse;
    }
}
