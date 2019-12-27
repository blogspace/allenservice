package com.demo.controller

import java.util

import org.apache.log4j.{Level, Logger}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession, functions}

object Sparkdemo {

  case class Stu(name: String, like: String)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sparkSession = SparkSession.builder().master("local").getOrCreate()
    val javasc = new JavaSparkContext(sparkSession.sparkContext)
    import org.apache.spark.sql.functions._

    val nameRDD1 = javasc.parallelize(util.Arrays.asList("{'id':'7'}"));
    val nameRDD1df = sparkSession.read.json(nameRDD1)
    val nameRDD2 = javasc.parallelize(util.Arrays.asList("{'id':'8'}"));
    val nameRDD2df = sparkSession.read.json(nameRDD2)
    val nameRDD3 = javasc.parallelize(util.Arrays.asList("{'id':'9'}"));
    val nameRDD3df = sparkSession.read.json(nameRDD3)
    val nameRDD4 = javasc.parallelize(util.Arrays.asList("{'id':'10'}"));
    val nameRDD4df = sparkSession.read.json(nameRDD4)
  val data =  nameRDD1df.union(nameRDD2df).union(nameRDD3df).union(nameRDD4df)
    data.withColumnRenamed("id","name").drop("name").show()
    // sparkSession.udf.register("myMax",new MyMax)
    //sparkSession.udf.register("myAvg", new MyAvg)

   // val data = sparkSession.sql("select myAvg(id) from idList").show(100)
  }
}

class MyAvg extends UserDefinedAggregateFunction {
  //输入数据的类型
  override def inputSchema: StructType = StructType(StructField("input", IntegerType) :: Nil)

  //中间结果数据的类型
  override def bufferSchema: StructType = StructType(
    StructField("sum", IntegerType) :: StructField("count", IntegerType) :: Nil)

  //定义输入数据的类型
  override def dataType: DataType = IntegerType

  //规定一致性
  override def deterministic: Boolean = true

  //初始化操作
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0,0)
    buffer.update(1,0)
  }

  //map端reduce,所有数据必须过这一段代码
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer.update(0, buffer.getInt(0) + input.getInt(0))
    buffer.update(1, buffer.getInt(1) + 1)
  }

  //reduce数据，update里面Row，没有第二个字段，这时候就有了第二个字段
  override def merge(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer.update(0, buffer.getInt(0) + input.getInt(0))
    buffer.update(1, buffer.getInt(1) + input.getInt(1))
  }

  //返回最终结果
  override def evaluate(finalVaue: Row): Int = {
    finalVaue.getInt(0) / finalVaue.getInt(1)
  }
}


