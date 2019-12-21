package com.controller

import java.util.concurrent.{Callable, Executors}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{SQLContext, SparkSession}

object Sparkdemo {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("demo").master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    spark.sql("use log")
    spark.sql("select count(*) from demo").show()
    spark.stop()
  }


}
