package com.service;

import com.config.Config;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;

public class HbaseSink implements OutputFormat<Tuple3<Long, Long, String>> {
    private org.apache.hadoop.conf.Configuration conf;
    private Connection connection = null;
    private Table table = null;

    public void configure(Configuration configuration) {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", Config.value("hbase.zookeeper.quorum"));
        conf.set("hbase.zookeeper.property.clientPort", Config.value("hbase.zookeeper.property.clientPort"));
        conf.set("hbase.rpc.timeout", Config.value("hbase.rpc.timeout"));
        conf.set("hbase.client.operation.timeout", Config.value("hbase.client.operation.timeout"));
        conf.set("hbase.client.scanner.timeout.period", Config.value("hbase.client.scanner.timeout.period"));
    }

    public void open(int i, int i1) throws IOException {
        connection = ConnectionFactory.createConnection(conf);
    }

    public void writeRecord(Tuple3<Long, Long, String> record) throws IOException {
        Put put = new Put(Bytes.toBytes(record.f0 + record.f0));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("uerid"), Bytes.toBytes(record.f0));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("behavior"), Bytes.toBytes(record.f2));
        ArrayList<Put> putList = new ArrayList<>();
        putList.add(put);
        //设置缓存1m，当达到1m时数据会自动刷到hbase
        BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(Config.value("hbase.table.name")));
        params.writeBufferSize(1024 * 1024); //设置缓存的大小
        BufferedMutator mutator = connection.getBufferedMutator(params);
        mutator.mutate(putList);
        mutator.flush();
        putList.clear();
    }

    public void close() throws IOException {
        if (table != null) {
            table.close();
        }
        if (connection != null) {
            connection.close();
        }
    }
}
