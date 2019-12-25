package com.Binarizer

// $example on$
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.Binarizer
// $example off$
import org.apache.spark.sql.SparkSession

/**
  * 二值化
  */
object BinarizerDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf();
    sparkConf.setMaster("local[*]").setAppName(this.getClass.getSimpleName)
    val spark = SparkSession
      .builder
      .config(sparkConf)
      .appName("BinarizerExample")
      .getOrCreate()

    // $example on$
    val data = Array((0, 0.1), (1, 0.8), (2, 0.6))
    val dataFrame = spark.createDataFrame(data).toDF("id", "feature")
    // transform 开始转换,将该列数据二值化，大于阈值的为1.0，否则为0.0
    val binarizer: Binarizer = new Binarizer()
      .setInputCol("feature")
      .setOutputCol("binarized_feature")
      .setThreshold(0.5)

    val binarizedDataFrame = binarizer.transform(dataFrame)

    println(s"Binarizer output with Threshold = ${binarizer.getThreshold}")
    binarizedDataFrame.show()
    // $example off$

    spark.stop()
  }
}