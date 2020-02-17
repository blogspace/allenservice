package com.controller

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


object Demo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("demo").setMaster("local[*]")
    val sc = new SparkContext(conf)
  }


}
