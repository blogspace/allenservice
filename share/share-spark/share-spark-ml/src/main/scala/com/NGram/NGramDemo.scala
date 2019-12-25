package com.NGram


// $example on$
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.NGram
// $example off$
import org.apache.spark.sql.SparkSession

/**
  * N-Gram认为语言中每个单词只与其前面长度 N-1 的上下文有关。主要分为bigram和trigram，
  * bigram假设下一个词的出现依赖它前面的一个词，trigram假设下一个词的出现依赖它前面的两个词。
  * 在SparkML中用NGram类实现，setN(2)为bigram，setN(3)为trigram。
  */
object NGramDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf();
    sparkConf.setMaster("local[*]").setAppName(this.getClass.getSimpleName)
    val spark = SparkSession
      .builder
      .config(sparkConf)
      .appName("NGramExample")
      .getOrCreate()

    // $example on$
    val wordDataFrame = spark.createDataFrame(Seq(
      (0, Array("Hi", "I", "heard", "about", "Spark")),
      (1, Array("I", "wish", "Java", "could", "use", "case", "classes")),
      (2, Array("Logistic", "regression", "models", "are", "neat"))
    )).toDF("id", "words")

    val ngram = new NGram().setN(2).setInputCol("words").setOutputCol("ngrams")

    val ngramDataFrame = ngram.transform(wordDataFrame)
    ngramDataFrame.select("ngrams").show(false)
    // $example off$

    spark.stop()
  }
}
