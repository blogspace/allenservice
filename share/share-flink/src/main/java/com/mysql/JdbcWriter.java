package com.mysql;

import com.entity.StudentDemo;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 *
 */
public class JdbcWriter extends RichSinkFunction<StudentDemo> {
    private Connection connection;
    private PreparedStatement ps;
    Config config = ConfigFactory.load("my.properties");

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName(config.getString("jdbc.driver"));//加载数据库驱动
        connection = DriverManager.getConnection(
                config.getString("jdbc.url"),
                config.getString("jdbc.user"),
                config.getString("jdbc.password"));//获取连接
        String sql = "insert into Student(stuid,stuname,stuaddr,stusex)values(?,?,?,?);";
        ps = connection.prepareStatement(sql);
        super.open(parameters);
    }

    public void close() throws Exception {
        super.close();
        if (ps != null) {
            ps.close();
        }
        if (connection != null) {
            connection.close();
        }
        super.close();
    }

    public void invoke(StudentDemo demo, Context context) throws Exception {
        try {
            int stuid = demo.getStuid();
            String name = demo.getStuname();//获取JdbcReader发送过来的结果
            String stuaddr = demo.getStuaddr();
            String stusex = demo.getStusex();
            ps.setInt(1, stuid);
            ps.setString(2, name);
            ps.setString(3, stuaddr);
            ps.setString(4, stusex);
            ps.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
