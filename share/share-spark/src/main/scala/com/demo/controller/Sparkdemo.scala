package com.demo.controller

import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext, SparkSession, functions}

object Sparkdemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("demo").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    loadDB(sqlContext,properties)//.registerTempTable("tmp")
    //sqlContext.sql("select * from tmp limit 5").show()
  }
  def loadDB(sqlContext: SQLContext, prop: Properties) = {
    sqlContext.read.format("jdbc").option("url", prop.getProperty("jdbc.url"))
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", prop.getProperty("jdbc.table"))
      .option("user", prop.getProperty("jdbc.user"))
      .option("password", prop.getProperty("jdbc.password"))
      .load()
  }
  def properties(): Properties = {
    val props = new Properties()
    val in = this.getClass.getClassLoader.getResourceAsStream("my.properties")
    props.load(in)
    props
  }

}


