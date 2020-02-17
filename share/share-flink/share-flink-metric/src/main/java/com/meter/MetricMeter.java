package com.meter;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MetricMeter {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource source = env.socketTextStream("localhost", 6666);
        source.map(new RichMapFunction<Long, Long>() {
            Counter counter;
            Meter meter;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                getRuntimeContext().getMetricGroup().addGroup("flink-metric-meter").meter("meter", new MeterView(counter,20));
            }

            public Long map(Long value) throws Exception {
                counter.inc();
                return value;
            }
        });

    }
}
