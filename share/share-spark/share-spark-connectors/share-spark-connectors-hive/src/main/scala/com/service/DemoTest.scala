package com.service

import java.io.IOException
import java.net.URI

import org.apache.hadoop
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

object DemoTest {
  var fileSystem: FileSystem = _
  var status: Array[FileStatus] = _
  var arr = new ArrayBuffer[String]()

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.jetty.server").setLevel(Level.OFF)
    val spark = SparkSession.builder().appName("hive").master("local[*]")
      .config("spark.sql.warehouse .dir", "hdfs://datacluster:9000/hive/warehouse")
      .config("spark.cleaner.referenceTracking.cleanCheckpoints", true)
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.textFile("").persist(StorageLevel.MEMORY_AND_DISK_SER_2)
    spark.sparkContext.setCheckpointDir("hdfs://datacluster:9000/checkpoint")
    //    val value = spark.sql("show create table dwd.dwd_c_fund_order_member_info").rdd.foreach(x=>println(x))
    //    val sa = spark.sql("show create table demo.sequencefile_test")


    //    val dataframe1 = spark.sql("select * from dwd.dwd_c_fund_order_member_info where staticdate='2020-08-02'")
    //    dataframe1.createOrReplaceTempView("dataframe1")


    import spark.implicits._
    //    val table = "create table if not exists log.logClean(timestr string,offsetNum string,app_user_id string,vid string,time string,fromtype string,url string,referrer_url string,event string,type string,app_id string,channel_id string,device_id string,extra_id string)" +
    //      "COMMENT 'Web Access Log'" +
    //      "ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'" +
    //      "STORED AS TEXTFILE"
    //    spark.sql("create database if not exists log")
    //    spark.sql("show databases").show()
    //    spark.sql("drop table if exists logClean ")
    //    spark.sql(table)
    //    spark.sql("LOAD DATA INPATH 'hdfs://datanode:9000/log' INTO TABLE log.logClean")
    //"LOAD DATA LOCAL INPATH 'D:\admin\Desktop\log' INTO TABLE log.logClean"
    //        spark.sql("drop table if exists log.logClean")

    //  val data = spark.sql("select * from log.logclean")
    //        val sqlStr="CREATE EXTERNAL  TABLE if not exists  dws.dws_c_fund_order_member_info( `id` bigint, `trade_id` string, `uid` bigint, `to_uid` bigint, `amount` double, `balance_amount` double, `inter_rate` double, `type` bigint, `state` bigint, `object_type` bigint, `object_id` bigint, `days` bigint, `coupon_id` bigint, `app_type` bigint, `create_time` bigint, `is_sandbox` bigint, `coupon_type` bigint, `abctime_level` bigint, `refresh_time` string, `pay_time` bigint, `member_type` string, `project` bigint)  PARTITIONED BY (`staticdate` string) STORED AS ORC"
    //        spark.sql(sqlStr)

    //    spark.sql("create database if not exists demo")

    //        spark.sql("show databases").show()

    //    spark.sql("desc table dws.dws_c_fund_order_member_info").show(25)
    //    val data = spark.read.orc("D:\\admin\\Desktop\\demoTest\\data")
    //    val data = spark.read.orc("D:\\admin\\Desktop\\demoTest\\channel")
    //    data.show(35)


    //测试

    //    val data2 = spark.read.orc("hdfs://datacluster:9000/hive/warehouse/dwd.db/dwd_c_fund_order_member_info/staticdate=2020-08-05/project=1")
    //
    //    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    //    data2.checkpoint().repartition(2) //设置为一个partition, 这样可以把输出文件合并成一个文件
    //      .write.mode(SaveMode.Overwrite)
    //      .format("orc")
    //      .option("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    //      .save("hdfs://datacluster:9000/hive/warehouse/dwd.db/dwd_c_fund_order_member_info/staticdate=2020-08-05/project=1")
    //    val dataframe2 = spark.sql("select * from dwd.dwd_c_fund_order_member_info where staticdate='2020-08-02'")
    //    dataframe2.createOrReplaceTempView("dataframe2")
    //    spark.sql("select * from dataframe1 a left join dataframe2 b on a.trade_id=b.trade_id and a.uid=b.uid and a.create_time=b.create_time where b.trade_id is null")
    //      .show(35)
    //    val dataFrame = spark.sql("select * from dws.dws_c_fund_order_member_info")
    //    dataFrame.checkpoint(true)
    //      .write.format("orc").mode("overwrite")
    //      .partitionBy("staticdate", "project")
    //      .saveAsTable("dwd.dwd_c_fund_order_member_info")


    //    spark.read
    //      .parquet("hdfs://datacluster:9000/hive/warehouse/dwd.db/dwd_c_fund_order_member_info/staticdate=2020-08-01/project=1/*")
    //      .show()

    //    spark.sparkContext.textFile("hdfs://datacluster:9000/hive/warehouse/dwd.db/dwd_c_fund_order_member_info/staticdate=2020-08-01/project=1/*")
    //      .filter(x => x.contains("ORC") || x.contains("parquet")).map(x => {
    //      x match {
    //        case _ if x.contains("ORC") == true => "orc"
    //        case _ => "parquet"
    //      }
    //
    //    }).distinct()
    //    val srcc = "hdfs://datacluster:9000/hive/warehouse/dwd.db/dwd_c_fund_order_member_info/staticdate=2020-08-01/project=1/*"
    //    val format = dataFormat(spark, srcc)
    //    val text = spark.sqlContext.sql("show create table dwd.dwd_c_fund_order_member_info").rdd
    //    text.foreach(x=>println)


    //    println(text)
    //    spark.read.format(format).load(srcc).show()
    //    val data2 = spark.read.orc("hdfs://datacluster:9000/hive/warehouse/dwd.db/dwd_c_fund_order_member_info/staticdate=2020-08-02")
    //
    //    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    //
    //    data2.checkpoint().coalesce(1) //设置为一个partition, 这样可以把输出文件合并成一个文件
    //      .write.mode(SaveMode.Overwrite)
    //      .format("orc")
    //      .save("hdfs://datacluster:9000/hive/warehouse/dwd.db/dwd_c_fund_order_member_info/staticdate=2020-08-02")
    //    val dataframe2 = spark.sql("select * from dwd.dwd_c_fund_order_member_info where staticdate='2020-08-02'")
    //    dataframe2.createOrReplaceTempView("dataframe2")
    //    spark.sql("select * from dataframe1 a left join dataframe2 b on a.trade_id=b.trade_id and a.uid=b.uid and a.create_time=b.create_time where b.trade_id is null")
    //      .show(35)

    //    val dataFrame = spark.sql("select * from dws.dws_c_fund_order_member_info")
    //    dataFrame.checkpoint(true)
    //      .write.format("orc").mode("overwrite")
    //      .partitionBy("staticdate")
    //      .saveAsTable("dwd.dwd_c_fund_order_member_info")


    //   spark.sql("INSERT overwrite table dwd.dwd_c_fund_order_member_info partition(staticdate) select * from dwd.dwd_c_fund_order_member_info")
    //   spark.sql("INSERT overwrite table dwd.dwd_c_fund_order_member_info partition(staticdate) select * from table")
    //    val data = spark.read.orc("D:\\admin\\Desktop\\demoTest\\data")
    //    data.coalesce(1).write.format("orc").mode("overwrite").save("D:\\admin\\Desktop\\demoTest\\test")
    //    val dataFrame = spark.sql("select * from dwd.dwd_c_fund_order_member_info")
    //    dataFrame.createOrReplaceTempView("table")
    //    Dataset.write.mode(SaveMode.Overwrite).insertInto("dwd_member")
    //      dataFrame.coalesce(1).write.format("orc").mode("overwrite").partitionBy("staticdate").saveAsTable("dwd.dwd_c_fund_order_member_info")
    //    spark.sql("select * from table")
    //      .write.format("orc").mode("overwrite")
    //      .partitionBy("staticdate")
    //      .saveAsTable("dwd.dwd_c_fund_order_member_info")
    //    spark.table("dwd.dwd_c_fund_order_member_info").show()
    //    val data = Array(("001", "张三", 21, "2018"), ("002", "李四", 18, "2017"))
    //
    //    val df = spark.createDataFrame(data).toDF("id", "name", "age", "year")
    //    //创建临时表
    //    df.createOrReplaceTempView("temp_table")
    //
    //    //切换hive的数据库
    //    spark.sql("use demo")
    //    //    1、创建分区表，可以将append改为overwrite，这样如果表已存在会删掉之前的表，新建表
    //    df.write.mode("overwrite").partitionBy("year").saveAsTable("demo.new_test_partition")
    //2、向Spark创建的分区表写入数据
    //    df.write.mode("append").partitionBy("year").saveAsTable("new_test_partition")
    //    sql("insert into new_test_partition select * from temp_table")
    //    df.write.insertInto("new_test_partition")


    fileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    //    val path = "hdfs://datacluster:9000/hive/warehouse/dwd.db/dwd_c_fund_order_member_info/staticdate=2020-08-01"
    //    val size = computePartitionNum(fileSystem, path, 128)
    //    val filesize = fileSystem.getContentSummary(new Path(path)).getLength
    //    println(filesize)


    //   val nums = listDirPath(spark.sparkContext, fileSystem, src)

    //    val demo = fileSystem.listFiles(new Path(src), true)
    //    val test = fileSystem.listStatus(new Path(src2))
    //
    //    val arr = ArrayBuffer[String]()
    //
    //    val paths = fileSystem.listStatus(new Path(path)).flatMap(dirPath => {
    //      listPath(dirPath, arr)
    //      arr
    //    }).distinct.foreach(x=>println)
    //    test.foreach(x=>println("打印："+x))
    //   println(fileSystem.listStatus(new Path(src3)).size)
    //    customUrl(spark, "dwd.dwd_c_fund_order_member_info").foreach(x => {
    //
    //
    //    })
    val arrrr = customUrl1(spark, "dwd.dwd_c_fund_order_member_info")
    //    arrrr.foreach(x=>println(x))
    //    val paths = arrrr.filter(path => {
    //      val fileUrl = fileSystem.listStatus(new Path(path.toString)).map(x => x.getPath.toString).apply(0)
    //      val res = fileUrl match {
    //        case _ if new Regex("(?<=\\.)snappy.orc").findFirstIn(fileUrl).getOrElse("other") == "other" => true
    //        case _ if new Regex("(?<=\\.)snappy.parquet").findFirstIn(fileUrl).getOrElse("other") == "other" => true
    //        case _ => true
    //      }
    //      res
    //    })

    //    fileSystem.listStatus(new Path(path)).foreach(x => println(x.getBlockSize))
    println("--------------")
    val path = "hdfs://192.168.110.10:9000/hive/warehouse/dws.db/dws_c_fund_order_member_info"

    fileSystem.listStatus(new Path(path)).foreach(x => println(x))
    println("--------------")
    //   println(Demo.getHDFSBlocks(fileSystem,spark,"dwd.dwd_c_fund_order_member_info"))
    val src1 = "hdfs://192.168.110.10:9000/hive/warehouse/demo.db/new_test_partition/year=2017/year=2017/"
    val src2 = "hdfs://192.168.110.10:9000/hive/warehouse/demo.db/text_test/"
//    fileSystem.rename(new Path(src1), new Path(src2))


  }

  def statistics(spark: SparkSession, execTable: String) = {
    spark.sql(s"select count(*) from ${execTable}").first().apply(0)
    spark.sql(s"select count(*) from ${execTable}").toJSON.apply("")

  }


  def customUrl1(spark: SparkSession, srcTable: String) = {

    //1.根据表名获取表的url地址
    val db = new Regex(".*(?=\\.)").findFirstIn(srcTable).getOrElse("other")
    val table = new Regex("(?<=\\.).*").findFirstIn(srcTable).getOrElse("other")
    println("db:" + db)
    println("table:" + table)

    val url = spark.catalog.getDatabase(db).locationUri
    val combineUrl = url + "/" + table

    //2.获取表下所有目录的全路径
    val arr = ArrayBuffer[String]()
    val dirPaths = fileSystem.listStatus(new Path(combineUrl)).flatMap(dirPath => {
      listPath(dirPath, arr)
      arr
    }).distinct.filter(path => fileSystem.listStatus(new Path(path.toString)).length > 0)
    dirPaths
  }

  def dataFormat(spark: SparkSession, src: String) = {
    spark.sparkContext.textFile(src)
      .filter(lines => lines.contains("ORC") || lines.contains("parquet")).map(line => {
      line match {
        case _ if line.contains("ORC") == true => "orc"
        case _ => "parquet"
      }
    }).distinct().collect().apply(0)
  }

  def dataFormat1(spark: SparkSession, src: String) = {
    spark.sparkContext.textFile(src)
      .filter(lines => lines.contains("ORC") || lines.contains("parquet")).map(line => {
      line match {
        case _ if line.contains("ORC") == true => "orc"
        case _ => "parquet"
      }
    }).distinct().collect().apply(0)
  }

  def storageFormat(spark: SparkSession, execTable: String) = {
    val statement = spark.sql(s"show create table ${execTable}").collect().mkString("\t")
    statement match {
      case _ if statement.contains("Orc") == true => "orc"
      case _ if statement.contains("Parquet") == true => "parquet"
      case _ if statement.contains("Text") == true => "text"
      case _ => "other"
    }
  }


  def customUrl(spark: SparkSession, sourceTable: String) = {

    //1.获取表的hdfsUrl地址
    val db = new Regex(".*(?=\\.)").findFirstIn(sourceTable).getOrElse("other")
    val table = new Regex("(?<=\\.).*").findFirstIn(sourceTable).getOrElse("other")
    println("db:" + db)
    println("table:" + table)

    val url = spark.catalog.getDatabase(db).locationUri
    val combineUrl = url + "/" + table

    //2.递归获取表下所有的目录路径
    val arr = ArrayBuffer[String]()
    val dirPaths = fileSystem.listStatus(new Path(combineUrl)).flatMap(dirPath => {
      listPath(dirPath, arr)
      arr
    }).distinct

    //3.过滤合并过的文件目录
    dirPaths.filter(x => fileSystem.listStatus(new Path(x.toString)).length > 0).filter(path => {

      val fileUrl = fileSystem.listStatus(new Path(path.toString)).map(x => x.getPath.toString).apply(0)
      val res = fileUrl match {
        case _ if new Regex("(?<=\\.)snappy.orc").findFirstIn(fileUrl).getOrElse("other") == "other" => true
        case _ if new Regex("(?<=\\.)snappy.parquet").findFirstIn(fileUrl).getOrElse("other") == "other" => true
        case _ => true
      }
      res
    })


  }

  //  def listPath(fileStatus: Array[FileStatus]) = {
  //    val arr = new ArrayBuffer[String]()
  //    fileStatus.foreach(x => {
  //      if (x.isDirectory) {
  //        status = fileSystem.listStatus(new Path(x.toString))
  //        listPath(status)
  //      }
  //      status
  //    })
  //
  //  }


  def listPath(fs: FileStatus, array: ArrayBuffer[String]): Unit = {
    val path = fs.getPath
    if (fs.isDirectory) {
      val sta = fileSystem.listStatus(new Path(path.toString)).filter(s => s.isDirectory)
      if (sta.length > 0) {
        sta.foreach(x => listPath(x, array))
      } else if (sta.length == 0) {
        array.append(path.toString)
      }
    }
  }


  def computePartitionNum(fileSystem: FileSystem, filePath: String, partitionSize: Int): Int = {
    val path = new Path(filePath)
    try {
      val filesize = fileSystem.getContentSummary(path).getLength
      val msize = filesize.asInstanceOf[Double] / 1024 / 1024 / partitionSize
      Math.ceil(msize).toInt

    } catch {
      case e: IOException => e.printStackTrace()
        1
    }
  }

  /**
    * 列出目录下文件绝对路径
    *
    * @param sc       SparkContext
    * @param filePath 文件路径 (String)
    * @return
    */
  //  def listDirPath(sc: SparkContext, fs: FileSystem, filePath: String) = {
  //    val fileStatus: Array[FileStatus] = fs.listStatus(new Path(filePath))
  //    var num =0
  //    fileStatus.map(
  //      status => {
  //
  //        status.getPath.toString
  //        if(status.isDirectory) {num+1}
  //        num
  //      }
  //
  //    )
  //  }


  def listDirPath(sc: SparkContext, fs: FileSystem, filePath: String) = {
    var num = 0
    fs.listStatus(new Path(filePath)).filter(x => x.isDirectory).size
  }


}
