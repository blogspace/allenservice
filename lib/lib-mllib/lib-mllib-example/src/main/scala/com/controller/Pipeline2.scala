package com.controller
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.ml.linalg.Vector

object Pipeline2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("pipeline2")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    //1.训练样本
    val training = spark.createDataFrame(Seq(
      (0L, "a b c d e spark", 1.0),
      (1L, "b d", 0.0),
      (2L, "spark f g h", 1.0),
      (3L, "hadoop mapreduce", 0.0)
    )).toDF("id", "text", "label")

    //2.ML pipeline参数设置，包括三个过程：首先是tokenizer，然后是hashingTF，最后是lr
    val tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("words")
    val hashingTF = new HashingTF()
      .setNumFeatures(1000)
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("features")
    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.001)
    val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, lr))

    //3.训练pipeline模型
    val model = pipeline.fit(training)

    //4.保存pipeline模型
    // model.write.overwrite().save("E://temp//one")

    //5.保存pipeline
    // pipeline.write.overwrite().save("E://temp//two")

    //6.加载pipeline模型
    //val sameModel = PipelineModel.load("E://temp//one")

    //7.测试样本
    val test = spark.createDataFrame(Seq(
      (4L, "spark i j k"),
      (5L, "l m n"),
      (6L, "spark hadoop spark"),
      (7L, "apache hadoop")
    )).toDF("id", "text")

    //8.模型测试
    model.transform(test)
      .select("id", "text", "probability", "prediction")
      .collect()
      .foreach{
        case Row(id: Long, text: String, prob: Vector, prediction: Double) =>
          println(s"($id. $text) --> prob=$prob, prediction=$prediction")
      }
  }

}
