package com.controller

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object Demo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //val data = env.createInput(new HBaseSink())

  }

}
