package com.counter;

import com.config.Configs;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

public class MetricCounter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource source = env.socketTextStream("localhost",6666);

        source.map(new RichMapFunction<String, String>() {
            Counter counter;
            int index;
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                index = getRuntimeContext().getIndexOfThisSubtask();
                counter = getRuntimeContext().getMetricGroup().addGroup("flink-metric-counter").counter("index" + index);
            }

            public String map(String s) throws Exception {
                counter.inc();
                System.out.println("index2 = " + (index + 1) + " counter1 = " + counter.getCount());

                return s;
            }
        }).print();

        env.execute("demo");
    }
}
