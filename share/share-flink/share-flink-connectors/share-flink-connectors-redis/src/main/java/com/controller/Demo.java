package com.controller;

import com.config.Config;
import com.service.RedisWriter;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

public class Demo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        DataStreamSource<String> data = env.addSource(new FlinkKafkaConsumer011<String>("topic", new SimpleStringSchema(), Config.kafkaProperties("demo"));

//        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("").build();
//        data.addSink(new RedisSink<Tuple2<String, String>>(conf, new RedisWriter()));


    }
}
