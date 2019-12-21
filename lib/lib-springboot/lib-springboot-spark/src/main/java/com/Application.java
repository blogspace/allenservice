package com;

import com.config.SparkConfig;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Application implements CommandLineRunner {
    @Autowired
    SparkConfig sparkConfig;

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        SparkConf conf = new SparkConf()
                .setAppName(sparkConfig.getAppName())
                .setMaster(sparkConfig.getMaster());
                //.set("spark.jars", "D:\\workspace\\idea\\DataProject\\lib\\lib-springboot\\lib-springboot-spark\\target\\lib-springboot-spark-0.0.1-SNAPSHOT.jar");

        //.set("spark.jars","lib-springboot-spark-0.0.1-SNAPSHOT.jar");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);//D:\admin\Desktop\log
        Dataset<Row> data = sqlContext.read().text("hdfs://datanode:9000/data/dwo/logClean/*");
        data.show(100);
        System.out.println("hello world");
        sc.stop();
    }
}
