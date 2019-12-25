package com.StandardScaler

// $example on$
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.StandardScaler
// $example off$
import org.apache.spark.sql.SparkSession

object StandardScalerDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf();
    sparkConf.setMaster("local[*]").setAppName(this.getClass.getSimpleName)
    val spark = SparkSession
      .builder
      .config(sparkConf)
      .appName("StandardScalerExample")
      .getOrCreate()

    // $example on$
    val dataFrame = spark.read.format("libsvm").load("D:/java/spark/spark/data/mllib/sample_libsvm_data.txt")

    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .setWithStd(true)
      .setWithMean(false)

    // Compute summary statistics by fitting the StandardScaler.
    val scalerModel = scaler.fit(dataFrame)

    // Normalize each feature to have unit standard deviation.
    val scaledData = scalerModel.transform(dataFrame)
    scaledData.show()
    // $example off$

    spark.stop()
  }
}