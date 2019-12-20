package com.controller;

import com.dao.AdPojo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

/**
 * flinkSql批处理
 */
public class BatchDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        final EnvironmentSettings bSetting = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);

        DataSet<AdPojo> csvInput = env.readCsvFile("D:\\admin\\Desktop\\data.csv")
                .ignoreFirstLine() //忽略第一行
                .pojoType(AdPojo.class, "channel", "subject", "refer", "reg", "ord", "pv", "uv");;
        //3）将DataSet转换为Table，并注册为table1
        Table topScore = tableEnv.fromDataSet(csvInput);
        tableEnv.registerTable("table1", topScore);

        //4）自定义sql语句
        Table groupedByCountry = tableEnv.sqlQuery("select channel,subject,refer,reg,ord,pv,uv from table1");
        //5）转换回dataset
        DataSet<AdPojo> result = tableEnv.toDataSet(groupedByCountry, AdPojo.class);
        //6）打印
        result.print();
    }
}
