package com.service;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.stereotype.Service;

@Service
public class FlinkService {

    public void index()  {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("datanode",8081);
        DataStreamSource<String> data = env.socketTextStream("datanode", 6666);
        data.print();
        try {
            env.execute("flinkdemo");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
