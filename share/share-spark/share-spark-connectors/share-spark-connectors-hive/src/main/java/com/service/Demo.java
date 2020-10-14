package com.service;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Demo {
//    public static void main(String[] args) {
//        SparkConf sparkConf = new SparkConf().setAppName("test").setMaster("local[*]");
//        JavaSparkContext sc = new JavaSparkContext(sparkConf);
//
//    }

//    public static void main(String[] args) {
//        Demo s = new Demo();
//        s.test();
//    }

    public void test() {
        Runtime run = Runtime.getRuntime();
        try {
            Process p = run.exec("ping 127.0.0.1");
            InputStream ins = p.getInputStream();
            InputStream ers = p.getErrorStream();
            new Thread(new inputStreamThread(ins)).start();
            p.waitFor();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    class inputStreamThread implements Runnable {
        private InputStream ins = null;
        private BufferedReader bfr = null;

        public inputStreamThread(InputStream ins) {
            this.ins = ins;
            this.bfr = new BufferedReader(new InputStreamReader(ins));
        }

        public void run() {
            String line = null;
            byte[] b = new byte[100];
            int num = 0;
            try {
                while ((num = ins.read(b)) != -1) {
                    System.out.println(new String(b, "gb2312"));
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

//    public static int getHDFSBlocks(FileSystem fs, Path path) throws Exception {
//        int totalFiles = 0;        // 总文件数
//        int totalBlocks = 0;    // 总数据块数
//        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(path, true);
//        while (listFiles.hasNext()) {
//            LocatedFileStatus next = listFiles.next();
//            int length = next.getBlockLocations().length;
//            totalBlocks += length;
//            if (next.getLen() != 0) {
//                totalFiles++;
//            }
//        }
//        return totalBlocks;
//    }

    /**
     * 获取指定目录下数据块个数
     *
     * @param fs
     * @param path
     * @return
     * @throws Exception
     */
    public static int getHDFSBlocks(FileSystem fs, SparkSession sparkSession, String srcTable) throws Exception {
        int totalFiles = 0;
        int totalBlocks = 0;
        String table = "";
        String db = srcTable.substring(0, 3);
        Matcher tablePattern = Pattern.compile("(?<=\\.).*").matcher(srcTable);
        while (tablePattern.find()) {
            table = tablePattern.group();
        }

        String url = sparkSession.catalog().getDatabase(db).locationUri();
        String combineUrl = url + "/" + table;

        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path(combineUrl), true);
        while (listFiles.hasNext()) {
            LocatedFileStatus next = listFiles.next();
            int length = next.getBlockLocations().length;
            totalBlocks += length;
            if (next.getLen() != 0) {
                totalFiles++;
            }
        }
        return totalBlocks;
    }

    public static void main(String[] args) {
        String db = "dws.dws_c_fund_order_member_info";

//        Matcher tablePattern = Pattern.compile("(?<=\\.).*").matcher("dws.dws_c_fund_order_member_info");
//        while (tablePattern.find()) {
//            table = tablePattern.group();
//        }


        System.out.println(db.substring(0, 3));


    }
}
