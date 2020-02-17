package com.service

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext, SaveMode, SparkSession}
import org.apache.spark.util.Utils

case class logschema(timestr: String, offsetNum: Int, app_user_id: String, vid: String, time: String, fromtype: String, url: String, referrer_url: String, event: String, typed: String, app_id: String, channel_id: String, device_id: String, extra_id: String)

//timestr(string) ,offsetNum(int),app_user_id(string) ,vid(string) ,time(string) ,fromtype (string) ,url (string) ,referrer_url (string),event (string) ,type (string) ,app_id (string) ,channel_id (string) ,device_id (string) ,extra_id (string)
object DemoTest {

  case class Cust(id: Integer, name: String, sales: Double, discount: Double, state: String)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("demo").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //spark.conf.set("spark.sql.shuffle.partitions","5")
    import sqlContext.implicits._

    val partitons = new Array[String](3)
    partitons(0) = "1=1 limit 0,10000"
    partitons(1) = "1=1 limit 10000,10000"
    partitons(2) = "1=1 limit 20000,10000"

//    val result = loadD(sqlContext, properties(), partitons).rdd.getNumPartitions
    loadDB(sqlContext,properties).registerTempTable("tmp")
   val query = sqlContext.sql("select * from tmp where offsetNum >100 limit 10")
    query.write.parquet("D:\\admin\\Desktop\\demo\\test")
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

  /**
    * @function 多分区模式
    * @param sqlContext
    * @param prop
    * @param column
    * @return
    */
  def loadDB(sqlContext: SQLContext, prop: Properties, column: String) = {
    sqlContext.read.format("jdbc").option("url", prop.getProperty("jdbc.url"))
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", prop.getProperty("jdbc.table"))
      .option("user", prop.getProperty("jdbc.user"))
      .option("password", prop.getProperty("jdbc.password"))
      .option("partitionColumn", column)
      .option("lowerBound", 1)
      .option("upperBound", 10000)
      .option("numPartitions", 5)
      .option("fetchsize", 100)
      .load()
  }

  /**
    * @function 分页模式读取（高自由度分区）
    * @return
    */
  val partitons = new Array[String](3)
  partitons(0) = "1=1 limit 0,10000"
  partitons(1) = "1=1 limit 10000,10000"
  partitons(2) = "1=1 limit 20000,10000"

  def loadD(sqlContext: SQLContext, prop: Properties, predicates: Array[String]) = {
    val pro = new Properties()
    pro.setProperty("user", prop.getProperty("jdbc.user"))
    pro.setProperty("password", prop.getProperty("jdbc.password"))
    sqlContext.read.jdbc(prop.getProperty("jdbc.url"), prop.getProperty("jdbc.table"), predicates, pro)
  }

  //    val schemaData = new StructType()
  //      .add("timestr", "String")
  //      .add("offsetNum", "Int")
  //      .add("app_user_id", "String")
  //      .add("vid", "String")
  //      .add("time", "String")
  //      .add("fromtype", "String")
  //      .add("url", "String")
  //      .add("referrer_url", "String")
  //      .add("event", "String")
  //      .add("typed", "String")
  //      .add("app_id", "String")
  //      .add("channel_id", "String")
  //      .add("device_id", "String")
  //      .add("extra_id", "String")
}
