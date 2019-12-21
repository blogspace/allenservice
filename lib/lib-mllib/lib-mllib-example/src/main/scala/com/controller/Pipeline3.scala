package com.controller
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.ml.linalg.Vector

object Pipeline3 {
  def main(args: Array[String]): Unit = {
    //设置日志输出格式
    Logger.getLogger("org").setLevel(Level.WARN)

    //1.定义SparkSession
    val spark = SparkSession.builder()
      .appName("textdata")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    //2.读取数据
    val data = spark.read.format("csv")
      .option("delimiter", "|")
      .option("header", "true")
      .load("C://Users//Machenike//Desktop//xzw//doc_class.dat")
      .toDF("myapp_id", "typenameid", "typename", "myapp_word", "myapp_word_all")

    val toDouble = udf[Double, String](_.toDouble)
    val clearData = udf[String, String](_.replace(" ", ""))
    val data2 = data.withColumn("label", toDouble(data("typenameid")))
      .withColumn("myapp_word_all", clearData(data("myapp_word_all")))
      .withColumnRenamed("myapp_word_all", "text")
      .withColumnRenamed("myapp_id", "id")
      .select("id", "text", "label")

    //3.将数据分为训练数据和测试数据
    val splitData = data2.randomSplit(Array(0.8, 0.2))
    val training = splitData(0)  //训练数据
    val testing = splitData(1)  //测试数据

    //4.tokenizer、hashingTF、lr
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
    //对tokenizer、hashingTF、lr进行封装
    val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, lr))

    //5.训练模型
    val model = pipeline.fit(training)

    //6.保存pipeline模型
    //model.write.overwrite().save("E://temp//one")

    //7.保存pipeline
    //pipeline.write.overwrite().save("E://temp//two")

    //8.加载pipeline模型
    //val sameModel = PipelineModel.load("E://temp//one")

    //9.测试
    val testing2 = testing.select("id", "text")
    model.transform(testing2)
      .select("id", "text", "probability", "prediction")
      .collect()
      .take(5)
      .foreach{
        case Row(id: String, text: String, prob: Vector, prediction: Double) =>
          println(s"($id, $text) --> prob = $prob, prediction = $prediction")
      }
  }

}
