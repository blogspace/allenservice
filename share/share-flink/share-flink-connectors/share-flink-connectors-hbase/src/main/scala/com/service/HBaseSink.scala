//package com.service
//
//import org.apache.flink.addons.hbase.TableInputFormat
//import org.apache.flink.configuration.ConfigConstants
//import org.apache.hadoop.hbase.client.{Result, Scan}
//import org.apache.hadoop.hbase.util.Bytes
//
//class HBaseSink extends TableInputFormat[Tuple2[String, String]] {
//  // 表名
//  val tableName = "test-table"
//  // 列族
//  val cf = "someCf".getBytes(ConfigConstants.DEFAULT_CHARSET)
//  // 列名
//  val column = "someQual".getBytes(ConfigConstants.DEFAULT_CHARSET)
////  val reuse = new Tuple2[String, String]
//  override def getScanner: Scan = {
//    val scan = new Scan
////    scan.addColumn(cf, column)
//    scan
//  }
//
//  override def getTableName: String = tableName
//
//  override def mapResultToTuple(result: Result): (String, String) = {
////    val key = Bytes.toString(result.getRow)
////    val value = Bytes.toString(result.getValue(cf, column))
////    reuse.setField(key, 0)
////    reuse.setField(value, 1)
//    ("","")
//  }
//}
