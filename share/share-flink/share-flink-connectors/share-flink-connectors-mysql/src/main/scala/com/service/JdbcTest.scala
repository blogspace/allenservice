package com.service
import java.sql.{Connection, DriverManager, Statement}

import com.dao.Student
import com.typesafe.config.ConfigFactory

object JdbcTest {
  def main(args: Array[String]) {

    val config = ConfigFactory.load("application.properties")
    val driver = config.getString("jdbc.driver")
    val url = config.getString("jdbc.url")
    val username = config.getString("jdbc.username")
    val password = config.getString("jdbc.password")

    var connection: Connection = null
    var statement: Statement = null
    try {
      //1.加载驱动
      Class.forName(driver)
      //2.创建连接
      connection = DriverManager.getConnection(url, username, password)
      //3.获得执行语句
      statement = connection.createStatement()
      //4.执行查询，获得结果集
      val resultSet = statement.executeQuery("select stuid,stuname,stuaddr,stusex from Student")
      //5.处理结果集
      while (resultSet.next()) {
        val student = Student(resultSet.getInt("stuid"), resultSet.getString("stuname").trim,
          resultSet.getString("stuaddr").trim, resultSet.getString("stusex").trim)
        println(student)
      }
    } catch {
      case e:Throwable => e.printStackTrace
    } finally {
      //6.关闭连接，释放资源
      if (connection != null) {
        connection.close()
      }
      if (statement != null) {
        statement.close()
      }
    }
    connection.close()
  }
}
