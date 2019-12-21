package com.config;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
public class ContextFactory {
//    @Autowired
//    SparkConfig sparkConfig;
//
//    @Bean
//    @ConditionalOnMissingBean(SparkConf.class)
//    public SparkConf sparkConf() throws Exception {
//        SparkConf conf = new SparkConf().setAppName(sparkConfig.getAppName()).setMaster(sparkConfig.getMaster());
//        conf.set("spark.jars","D:\\workspace\\idea\\DataProject\\lib\\lib-springboot\\lib-springboot-spark\\target\\lib-springboot-spark-0.0.1-SNAPSHOT.jar");
//        return conf;
//    }
//
//
//    @ConditionalOnMissingBean(JavaSparkContext.class)
//    public JavaSparkContext sparkContext() throws Exception {
//        return new JavaSparkContext(sparkConf());
//    }
//
//    @Bean
//    @ConditionalOnMissingBean(SQLContext.class)
//    public SQLContext sqlContext() throws Exception {
//        return new SQLContext(sparkContext());
//    }
}
