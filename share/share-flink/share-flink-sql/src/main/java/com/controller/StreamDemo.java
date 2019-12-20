package com.controller;

import com.dao.Order;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.util.Arrays;

public class StreamDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //final EnvironmentSettings fSetting = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        //final EnvironmentSettings bSetting = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        DataStream<Order> orderA = env.fromCollection(Arrays.asList(
                new Order(1L, "beer", 3),
                new Order(1L, "diaper", 4),
                new Order(3L, "rubber", 2)));

        DataStream<Order> orderB = env.fromCollection(Arrays.asList(
                new Order(2L, "pen", 3),
                new Order(2L, "rubber", 3),
                new Order(4L, "beer", 1)));

        // convert DataStream to Table
        Table tableA = tableEnv.fromDataStream(orderA, "user, product, amount");
        // register DataStream as Table
        tableEnv.registerDataStream("OrderB", orderB, "user, product, amount");
        Table table = tableEnv.sqlQuery("select * from " + tableA + "");

        // union the two tables
        Table result = tableEnv.sqlQuery("SELECT * FROM " + tableA + " WHERE amount > 2 UNION ALL " +
                "SELECT * FROM OrderB WHERE amount < 2");
        tableEnv.toAppendStream(table, Order.class).print();
        env.execute("demo");
    }

}

