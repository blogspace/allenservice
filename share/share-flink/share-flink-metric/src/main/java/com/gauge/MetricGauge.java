package com.gauge;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class MetricGauge {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> data = env.addSource(new SourceFunction<String>() {
            Boolean state;

            public void run(SourceContext<String> sourceContext) throws Exception {
                while (state = true) {
                    sourceContext.collect(String.valueOf(Math.random()));
                    Thread.sleep(1000);
                }
            }

            public void cancel() {
                state = false;
            }
        });

        data.map(new RichMapFunction<String, String>() {
            int value;
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                getRuntimeContext().getMetricGroup().addGroup("flink-metric-gauge").gauge("gauge",new Gauge<Integer>() {

                    public Integer getValue() {
                        return value;
                    }
                });
            }

            public String map(String s) throws Exception {
                value++;
                System.out.println("value:"+value);
                return s;
            }
        }).print();

        env.execute("demo");

    }
}
