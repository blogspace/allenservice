package com.service

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala.BatchTableEnvironment

object StudentSourceFromMysqlTest {

  def main(args: Array[String]): Unit = {
    //1.创建流执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.create(env)
    val data = env.readTextFile("D:\\admin\\Desktop\\log")
    val table = tableEnv.fromTableSource(data)
    tableEnv.registerTable("tables")
    tableEnv.sqlQuery("select * from tables")

    //3.显示结果
    // dataStream.print()

    //4.触发流执行
    env.execute()
  }

}
