package com.controller

import com.dao.Student
import com.service.{JDBCReader, JDBCWriter}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._


object JDBCDemo {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val data: DataStream[Student] = env.fromElements(
      Student(21, "dahua", "beijing biejing", "female"),
      Student(22, "daming", "tainjing tianjin", "male "),
      Student(23, "daqiang ", "shanghai shanghai", "female")
    )
    data.addSink(new JDBCWriter)

    val dataOutput: DataStream[Student] = env.addSource(new JDBCReader)
    dataOutput.print()
    env.execute("demo")

  }
}
