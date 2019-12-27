package com.dingding;


import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;


public class SendMessage {
    public static void main(String[] args) throws IOException {
        String WEBHOOK_TOKEN = "https://oapi.dingtalk.com/robot/send?access_token=xxxxx";
        String textMsg = "{ \"msgtype\": \"text\", \"text\": {\"content\": \"我就是我xxx, 是不一样的烟火\"}}";
        message(WEBHOOK_TOKEN,textMsg);
    }
    public static void message(String WEBHOOK_TOKEN,String msg) throws IOException {
        HttpClient httpclient = HttpClients.createDefault();
        HttpPost httppost = new HttpPost(WEBHOOK_TOKEN);
        httppost.addHeader("Content-Type", "application/json; charset=utf-8");
        StringEntity se = new StringEntity(msg, "utf-8");
        httppost.setEntity(se);
        HttpResponse response = httpclient.execute(httppost);
        if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
            String result = EntityUtils.toString(response.getEntity(), "utf-8");
            System.out.println(result);
        }
    }

}
