package com.controller;

import com.service.JdbcFormat;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

public class BatchDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
         RowTypeInfo rowTypeInfo = new RowTypeInfo(
                 BasicTypeInfo.INT_TYPE_INFO,
                 BasicTypeInfo.CHAR_TYPE_INFO,
                 BasicTypeInfo.CHAR_TYPE_INFO
                 );

         String sql = "select * from items";
         DataSource<Row> input = env.createInput(JdbcFormat.JDBCInput(rowTypeInfo,sql));
        input.map(new MapFunction<Row, String>() {
            @Override
            public String map(Row row) throws Exception {
                System.out.println(row);
                return row.toString();
            }
        }).print();

    }
}
