package com.service

import com.entity.StudentDemo
import com.mysql.{JdbcWriter, StudentSinkToMysql}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object StudentSinkToMysqlTest {
  def main(args: Array[String]): Unit = {

    //1.创建流执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //2.准备数据
    val dataStream = env.fromElements(
      new StudentDemo(9, "dahua", "beijing biejing", "female"),
     new StudentDemo(10, "daming", "tainjing tianjin", "male "),
      new StudentDemo(11, "daqiang ", "shanghai shanghai", "female")
    )

    //3.将数据写入到自定义的sink中（这里是mysql）
//    dataStream.addSink(new StudentSinkToMysql)
    dataStream.addSink(new JdbcWriter)

    //4.触发流执行
    env.execute()
  }
}
