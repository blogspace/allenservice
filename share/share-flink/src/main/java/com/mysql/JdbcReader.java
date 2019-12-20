package com.mysql;

import com.entity.StudentDemo;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class JdbcReader extends RichSourceFunction<StudentDemo> {
    private static final Logger logger = LoggerFactory.getLogger(JdbcReader.class);
    private Connection connection = null;
    private PreparedStatement ps = null;
    Config config = ConfigFactory.load("my.properties");

    //该方法主要用于打开数据库连接，下面的ConfigKeys类是获取配置的类
    public void open(Configuration parameters) throws Exception {
        Class.forName(config.getString("jdbc.driver"));//加载数据库驱动
        connection = DriverManager.getConnection(config.getString("jdbc.url"), config.getString("jdbc.user"), config.getString("jdbc.password"));//获取连接
        String sql = "select stuid,stuname,stuaddr,stusex from Student;";
        ps = connection.prepareStatement(sql);
    }

    //执行查询并获取结果
    public void run(SourceContext<StudentDemo> ctx) {
        try {
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                int stuid = resultSet.getInt("stuid");
                String stuname = resultSet.getString("stuname");
                String stuaddr = resultSet.getString("stuaddr");
                String stusex = resultSet.getString("stusex");
                StudentDemo studentDemo = new StudentDemo(stuid, stuname, stuaddr, stusex);
                ctx.collect(studentDemo);//发送结果，结果是tuple2类型，2表示两个元素，可根据实际情况选择
            }
        } catch (Exception e) {
            logger.error("runException:{}", e);
        }

    }

    //关闭数据库连接
    public void cancel() {
        try {
            super.close();
            if (connection != null) {
                connection.close();
            }
            if (ps != null) {
                ps.close();
            }
        } catch (Exception e) {
            logger.error("runException:{}", e);
        }
    }


}
