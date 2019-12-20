package com.controller;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class HbaseDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
       // DataStreamSource source = env.addSource(new FlinkKafkaConsumer011<String>("logTopic", new SimpleStringSchema(), Configs.kafkaProperties("demo")));

        env.execute("demo");


    }
}
