package com.service

import java.util.Properties

import com.config.Configs
import com.typesafe.config.ConfigFactory
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

object KafkaDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val config = ConfigFactory.load("application.properties")
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", config.getString("kafka.bootstrap.servers"))
    properties.setProperty("zookeeper.connect", config.getString("kafka.zookeeper.connect"))
    properties.setProperty("group.id", "demo")
   val data = env.addSource(new FlinkKafkaConsumer011[String]("logTopic", new SimpleStringSchema(), Configs.kafkaProperties("demo")))
    data.print()
    env.execute("demo")
  }
}
