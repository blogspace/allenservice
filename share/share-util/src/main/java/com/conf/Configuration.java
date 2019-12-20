package com.conf;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 配置管理组件
 * Created by yangtong on 17/5/5.
 */
public class Configuration {
    private static Properties prop = new Properties();

    static {
        try {
            InputStream in = Configuration.class.getClassLoader().getResourceAsStream("my.properties");
            //可以把key value对加载进去了
            prop.load(in);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 返回字符串类型
     *
     * @param key
     * @return 字符串类型的value
     */
    public static String getProperty(String key) {
        return prop.getProperty(key);
    }

    public static void get(String key) {
        // 通过ConfigFactory来获取到配置文件,默认加载配置文件的顺序是：application.conf --> application.json --> application.properties
        ConfigFactory.load().getString("key");

    }

}
