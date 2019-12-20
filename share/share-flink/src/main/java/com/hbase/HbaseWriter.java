package com.hbase;

import com.entity.StudentDemo;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class HbaseWriter extends RichSinkFunction<StudentDemo> {
    public void invoke(StudentDemo value) throws Exception {

    }

    public void invoke(StudentDemo value, Context context) throws Exception {

    }
}
