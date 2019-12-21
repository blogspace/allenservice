package com.service

import java.text.SimpleDateFormat

import com.alibaba.fastjson.JSON
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.spark.ml.feature.Tokenizer

import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex


object FlinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val src = env.socketTextStream("localhost", 6666)
    src.flatMap(x=>x.split(" ",-1)).map(x=>(x,1)).keyBy(0).sum(1).print()
//    val regex = new Regex("\\{\"msgs\":.*\\}]\\}")
    env.execute("flink")
//    val textFile = src.filter(x => x.contains("[ERROR]") && x.contains("prod") && regex.findAllIn(x.toString).length > 0)
//    val data = textFile.flatMap(cols => {
//      val col = cols.split(" \\[ERROR\\] ")
//      val timestr = col.apply(0)
//      val jsonstrArray = col.apply(1).split("_\\{")
//      val offsetNum = jsonstrArray.apply(0)
//      val jsonform = "{" + jsonstrArray.apply(1)
//      val timeArray = ArrayBuffer[String]()
//      timeArray += timestr += offsetNum
//      val json = JSON.parseObject(jsonform)
//      val msg = json.get("msgs").toString
//      val schema = Array("app_user_id", "vid", "time", "fromtype", "url", "referrer_url", "event", "type", "app_id", "channel_id", "device_id", "extra")
//      val schemaArray = valueData(msg, schema, timeArray)
//      schemaArray
//    })
//    data.map(x=>(x.split("\t",-1)(1),1))
//      .keyBy(0)
//      .timeWindow(Time.seconds(30))
//        .sum(1).print()
//    env.execute
//  }
//  def valueData(json: String, schema: Array[String], timeArray: ArrayBuffer[String]) = {
//    val jsonArray = JSON.parseArray(json).toArray
//    jsonArray.map(x => {
//      val json = JSON.parseObject(x.toString)
//      val array = ArrayBuffer[AnyRef]()
//      array ++= timeArray
//      for (arr <- schema) {
//        val value = json.getOrDefault(arr, "")
//        array += fieldmatch(arr, value).toString
//      }
//      array.mkString("\t")
//    })
//  }
//  val reg = new Regex("\"")
//  def fieldmatch(field: String, value: AnyRef) = field match {
//    case "time" => if (reg.findAllIn(value.toString) == false) {
//      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(value)
//    } else {
//      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(value.toString.replace("\"", "").toDouble)
//    }
//    case "extra" => if (value.toString.length > 2) {
//      JSON.parseObject(String.valueOf(value)).getOrDefault("id", "")
//    } else {
//      ""
//    }
//    //case "extra" => JSON.parseObject(String.valueOf(value)).getOrDefault("id","")
//    case _ => value
  }

  //  def dataClean(rdd: RDD[String]) = {
  //    val regex = new Regex("\\{\"msgs\":.*\\}]\\}")
  //    val textFile = rdd.filter(line => line.contains("[ERROR]") && line.contains("prod") && regex.findAllIn(line.toString).length > 0)
  //    val data = textFile.flatMap(cols => {
  //      val col = cols.split(" \\[ERROR\\] ")
  //      val timestr = col.apply(0)
  //      val jsonstrArray = col.apply(1).split("_\\{")
  //      val offsetNum = jsonstrArray.apply(0)
  //      val jsonform = "{" + jsonstrArray.apply(1)
  //      val timeArray = ArrayBuffer[String]()
  //      timeArray += timestr += offsetNum
  //      val json = JSON.parseObject(jsonform)
  //      val msg = json.get("msgs").toString
  //      val schema = Array("app_user_id", "vid", "time", "fromtype", "url", "referrer_url", "event", "type", "app_id", "channel_id", "device_id", "extra")
  //      val schemaArray = valueData(msg, schema, timeArray)
  //      schemaArray
  //    })
  //    data
  //  }

  //  val data = src.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks[String] {
  //      var currentMaxTimestamp = 0L
  //      val maxOutOfOrderness = 10000L
  //      var a: Watermark = null
  //      override def checkAndGetNextWatermark(x: String, y: Long) = {
  //        a = new Watermark(currentMaxTimestamp - maxOutOfOrderness)
  //        a
  //      }
  //      override def extractTimestamp(x: String, y: Long): Long = {
  //        val timestamp = x.split("\t", -1)(0)
  //        val time = dateToSeconds(timestamp, "yyyy-MM-dd HH:mm:ss")
  //        currentMaxTimestamp = Math.max(time, currentMaxTimestamp)
  //        //println("timestamp:"+timestamp+" "+"time:"+time+"  "+"max:"+currentMaxTimestamp+"  "+"Watermark:"+a)
  //        time
  //      }
  //    })
  //  def dateToSeconds(srcDate: String, srcDateFormat: String) = {
  //    val date: Date = new SimpleDateFormat(srcDateFormat).parse(srcDate)
  //    date.getTime
  //  }

}
