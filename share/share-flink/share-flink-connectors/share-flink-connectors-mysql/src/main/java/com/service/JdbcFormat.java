package com.service;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;

public class JdbcFormat {

   static Config config = ConfigFactory.load("application.properties");

    public static JDBCInputFormat JDBCInput(RowTypeInfo rowTypeInfo ,String sql) {
        JDBCInputFormat jdbcInput = JDBCInputFormat.buildJDBCInputFormat()
                .setDrivername(config.getString("jdbc.driver"))
                .setDBUrl(config.getString("jdbc.url"))
                .setUsername(config.getString("jdbc.username"))
                .setPassword(config.getString("jdbc.password"))
                .setQuery(sql)
                .setRowTypeInfo(rowTypeInfo)
                .finish();
        return jdbcInput;

    }

}