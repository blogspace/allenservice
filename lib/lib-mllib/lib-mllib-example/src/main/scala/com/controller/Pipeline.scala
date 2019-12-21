package com.controller

import org.apache.commons.logging.LogFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row

object Pipeline {
  def main(args: Array[String]): Unit = {
    //设置日志输出格式
    Logger.getLogger("org").setLevel(Level.ERROR)
    //定义SparkSession
    val spark = SparkSession.builder().appName("pipeline").master("local[*]").getOrCreate()
    import spark.implicits._

    //1、训练样本
    val training = spark.createDataFrame(Seq(
      (1.0, Vectors.dense(0.0, 1.1, 0.1)),
      (0.0, Vectors.dense(2.0, 1.0, -1.0)),
      (0.0, Vectors.dense(2.0, 1.3, 1.0)),
      (1.0, Vectors.dense(0.0, 1.2, -0.5))
    )).toDF("label", "features")
    val data = training.withColumn("test", training("label")).withColumnRenamed("features", "newFeatures")
   val Array(test,train)= data.randomSplit(Array(0.5,0.5))
    test.show()
    //2、创建逻辑回归Estimator
    //    val lr = new LogisticRegression()
    //
    //    //3、通过setter方法设置模型参数
    //    lr.setMaxIter(10) //设置最大迭代次数
    //      .setRegParam(0.01) //设置正则迭代因子
    //
    //    //4、训练模型
    //    val model1 = lr.fit(training)
    //
    //    //5、通过ParamMap设置参数方法
    //    val paramMap = ParamMap(lr.maxIter -> 20)
    //      .put(lr.maxIter, 30)
    //      .put(lr.regParam -> 0.1, lr.threshold -> 0.55)
    //    //ParamMap合并
    //    /* val paramMap2 = paramMap(lr.probabilityCol -> "myProbability")
    //     val paramMapCombined = paramMap ++ paramMap2*/
    //
    //    //6、训练模型，采用ParamMap参数
    //    //paramMapCombined会覆盖所有lr.set设置的参数
    //    //val model2 = lr.fit(training, paramMapCombined)
    //    val model2 = lr.fit(training, paramMap)
    //    //7、测试样本
    //    val test = spark.createDataFrame(Seq(
    //      (1.0, Vectors.dense(-1.0, 1.5, 1.3)),
    //      (0.0, Vectors.dense(3.0, 2.0, -0.1)),
    //      (1.0, Vectors.dense(0.0, 2.2, -1.5))
    //    )).toDF("label", "features")
    //
    //    //8、对模型进行测试
    //    model2.transform(test)
    //      .select("features", "label", "prediction")
    //      .collect()
    //      .foreach {
    //        case Row(features: Vector, label: Double, prediction: Double) =>
    //          println(s"($features. $label) -> prediction=$prediction")
    //      }
  }

}
