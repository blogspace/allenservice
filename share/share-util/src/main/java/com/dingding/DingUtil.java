package com.dingding;


import com.dingtalk.api.DefaultDingTalkClient;
import com.dingtalk.api.DingTalkClient;
import com.dingtalk.api.request.OapiUserGetRequest;
import com.dingtalk.api.response.OapiUserGetResponse;
import com.taobao.api.ApiException;

public class DingUtil {
    public static void main(String[] args) throws ApiException {
        String accessToken ="";
        DingTalkClient client = new DefaultDingTalkClient("https://oapi.dingtalk.com/user/get");
        OapiUserGetRequest req = new OapiUserGetRequest();
        req.setUserid("userid1");
        req.setHttpMethod("GET");
        OapiUserGetResponse rsp = client.execute(req, accessToken);

    }
}
