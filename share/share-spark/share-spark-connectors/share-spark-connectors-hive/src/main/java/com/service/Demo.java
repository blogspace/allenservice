package com.service;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Demo {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("test").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

    }
}
