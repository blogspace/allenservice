package com.hbase;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;


public class HbaseReader  extends RichSourceFunction<Tuple2<String, String>> {
    private static final Logger logger = LoggerFactory.getLogger(HbaseReader.class);
    private static org.apache.hadoop.conf.Configuration configuration;
    private static Connection connection = null;

    private Connection conn = null;
    private Table table = null;
    private Scan scan = null;


    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.master", "192.168.3.101:60020");
        configuration.set("hbase.zookeeper.quorum", "192.168.3.101");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        table = conn.getTable(TableName.valueOf(""));
        scan = new Scan();
        scan.setStartRow(Bytes.toBytes("1001"));
        scan.setStopRow(Bytes.toBytes("1004"));
        scan.addFamily(Bytes.toBytes(""));

    }


    public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {
        ResultScanner rs = table.getScanner(scan);

        Iterator<Result> iterator = rs.iterator();
        while (iterator.hasNext()) {
            Result result = iterator.next();
            String rowkey = Bytes.toString(result.getRow());
            StringBuffer sb = new StringBuffer();
            for (Cell cell : result.listCells()) {
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                sb.append(value).append(",");
            }
            String valueString = sb.replace(sb.length() - 1, sb.length(), "").toString();
            Tuple2<String, String> tuple2 = new Tuple2();
            tuple2.setFields(rowkey, valueString);
            ctx.collect(tuple2);
        }

    }


    public void cancel() {
        try {
            if (table != null) {
                table.close();
            }
            if (conn != null) {
                conn.close();
            }
        } catch (IOException e) {
            logger.error("Close HBase Exception:", e.toString());
        }
    }
}
