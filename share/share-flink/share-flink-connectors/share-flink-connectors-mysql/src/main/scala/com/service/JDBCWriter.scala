package com.service

import java.sql.{Connection, DriverManager, PreparedStatement}
import com.dao.Student
import com.typesafe.config.ConfigFactory
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

class JDBCWriter extends RichSinkFunction[Student] {
  private var connection: Connection = null
  private var ps: PreparedStatement = null

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val config = ConfigFactory.load("application.properties")
    val driver = config.getString("jdbc.driver")
    val url = config.getString("jdbc.url")
    val username = config.getString("jdbc.username")
    val password = config.getString("jdbc.password")

    Class.forName(driver)
    connection = DriverManager.getConnection(url, username, password)
    val sql = "insert into Student(stuid,stuname,stuaddr,stusex)values(?,?,?,?);"
    ps = connection.prepareStatement(sql)
  }

  /**
    * 二、每个元素的插入都要调用一次invoke()方法，这里主要进行插入操作
    */
  override def invoke(stu: Student): Unit = {
    try {
      //4.组装数据，执行插入操作
      ps.setInt(1, stu.stuid)
      ps.setString(2, stu.stuname)
      ps.setString(3, stu.stuaddr)
      ps.setString(4, stu.stusex)
      ps.executeUpdate()
    } catch {
      case e: Exception => println(e.getMessage)
    }
  }

  /**
    * 三、 程序执行完毕就可以进行，关闭连接和释放资源的动作了
    */
  override def close(): Unit = {
    super.close()
    //5.关闭连接和释放资源
    if (connection != null) {
      connection.close()
    }
    if (ps != null) {
      ps.close()
    }
  }
}