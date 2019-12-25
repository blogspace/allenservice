package com.PolynomialExpansion

// $example on$
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.PolynomialExpansion
import org.apache.spark.ml.linalg.Vectors
// $example off$
import org.apache.spark.sql.SparkSession

object PolynomialExpansionDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf();
    sparkConf.setMaster("local[*]").setAppName(this.getClass.getSimpleName)
    val spark = SparkSession
      .builder
      .config(sparkConf)
      .appName("PolynomialExpansionExample")
      .getOrCreate()

    // $example on$
    val data = Array(
      Vectors.dense(2.0, 1.0),
      Vectors.dense(0.0, 0.0),
      Vectors.dense(3.0, -1.0)
    )
    val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")
    //setDegree表示多项式最高次幂 比如1.0,5.0可以是 三次：1.0^3 5.0^3 1.0+5.0^2 二次：1.0^2+5.0 1.0^2 5.0^2 1.0+5.0 一次：1.0 5.0
    val polyExpansion = new PolynomialExpansion()
      .setInputCol("features")
      .setOutputCol("polyFeatures")
      .setDegree(3)

    val polyDF = polyExpansion.transform(df)
    polyDF.show(false)
    // $example off$

    spark.stop()
  }
}