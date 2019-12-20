package com.controller;

import com.dao.StudentDemo;
import com.service.JdbcReader;
import com.service.JdbcWriter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Demo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<StudentDemo> data = env.addSource(new JdbcReader());
        data.print();

        DataStream<StudentDemo> students = env.fromElements(
                new StudentDemo(27, "zhangsan", "beijing biejing", "female"),
                new StudentDemo(28, "lisi", "tainjing tianjin", "male ")
        );
        students.addSink(new JdbcWriter());
        env.execute("demo");

    }
}
