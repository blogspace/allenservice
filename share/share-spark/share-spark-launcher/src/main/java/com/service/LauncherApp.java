package com.service;

import com.util.InputStreamReaderRunnable;
import org.apache.spark.launcher.SparkLauncher;

import java.io.IOException;
import java.util.HashMap;


public class LauncherApp {
    public static void main(String[] args) throws IOException, InterruptedException {

        HashMap env = new HashMap();
        env.put("HADOOP_CONF_DIR","/usr/local/software/hadoop-2.9.2/etc/overriterHaoopConf");
        env.put("JAVA_HOME","/usr/local/software/jdk1.8.0_144");
        env.put("SPARK_HOME","/usr/local/software/spark-2.2.1-bin-hadoop2.7");
        //env.put("YARN_CONF_DIR","");

        SparkLauncher handle = new SparkLauncher(env)
                .setAppResource("/root/jars/RunRecommender-jar-with-dependencies.jar")
                .setMainClass("com.service.RunRecommender")
                .setMaster("spark://192.168.192.10:7077")
                .setDeployMode("client")
                //.setConf("spark.app.id", "11222")
                .setConf("spark.driver.memory", "1g")
                //.setConf("spark.akka.frameSize", "200")
                .setConf("spark.executor.memory", "1g")
                //.setConf("spark.executor.instances", "1")
                .setConf("spark.executor.cores", "2")
                .setConf("spark.default.parallelism", "150")
                .setConf("spark.driver.allowMultipleContexts","true")
                .setVerbose(true);


        Process process =handle.launch();
        InputStreamReaderRunnable inputStreamReaderRunnable = new InputStreamReaderRunnable(process.getInputStream(), "input");
        Thread inputThread = new Thread(inputStreamReaderRunnable, "LogStreamReader input");
        inputThread.start();

        InputStreamReaderRunnable errorStreamReaderRunnable = new InputStreamReaderRunnable(process.getErrorStream(), "error");
        Thread errorThread = new Thread(errorStreamReaderRunnable, "LogStreamReader error");
        errorThread.start();

        System.out.println("Waiting for finish...");
        int exitCode = process.waitFor();
        System.out.println("Finished! Exit code:" + exitCode);

    }


}
