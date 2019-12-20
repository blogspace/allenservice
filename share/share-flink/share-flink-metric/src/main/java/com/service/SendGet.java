package com.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;

//https://blog.csdn.net/aA518189/article/details/88952910
public class SendGet {
    public static String sendGet(String url) {
        String result = "";
        BufferedReader in = null;
        try {
            String urlNameString = url;
            URL realUrl = new URL(urlNameString);
            // 打开和URL之间的连接
            URLConnection connection = realUrl.openConnection();
            // 设置通用的请求属性
            connection.setRequestProperty("accept", "*/*");
            connection.setRequestProperty("connection", "Keep-Alive");
            connection.setRequestProperty("user-agent",
                    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
            // 建立实际的连接
            connection.connect();
            in = new BufferedReader(new InputStreamReader(
                    connection.getInputStream()));
            String line;
            while ((line = in.readLine()) != null) {
                result += line;
            }
        } catch (Exception e) {
            System.out.println("发送GET请求出现异常！" + e);
            e.printStackTrace();
        }
        // 使用finally块来关闭输入流
        finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
        return result;
    }

    public static void main(String[] args) {
        String url = "http://127.0.0.1:5004/proxy/application_1231435364565_0350/jobs";
        String url1 = "http://datanode:8081/#/jobs";
        String s = sendGet(url1);
        System.out.println(s);
//        JSONObject jsonObject = JSON.parseObject(s);
//        String string = jsonObject.getString("jobs");
//        String substring = string.substring(1, string.length() - 1);
//        JSONObject jsonObject1 = JSONObject.parseObject(substring);
//        String status = jsonObject1.getString("status");
//        System.out.println(status);
    }
}
