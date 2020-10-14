package com.controller

import java.text.SimpleDateFormat
import java.util.Date



object Demo {
  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setAppName("demo").setMaster("local[*]")
//    val sc = new SparkContext(conf)
    println(nowTime("2020-05-08"))
  }
  def nowTimeDemo(): Long = {
    var now: Date = new Date()
    var dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var nowDate = dateFormat.format()
    val timestamp = dateFormat.parse(nowDate).getTime/1000
    timestamp
  }
  def nowTime(date:String): Long = {
    var now = new SimpleDateFormat("yyyy-MM-dd").parse(date).getTime/1000
    now
  }



}
