package com.service


import org.apache.spark.{SparkConf, SparkContext}

object ProducerTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Kafkastset").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("error")
    val data = sc.textFile("D:\\admin\\Desktop\\log")

    //KafkaUtil.produces(data,"datanode:9092","logTopic")
    sc.stop()
  }

}
