package com.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.util.Properties;

public class Configs {
    static Properties properties = null;
    static Config config = ConfigFactory.load("application.properties");
    public static String key(String key) {
        return config.getString(key);
    }
    public static Properties kafkaProperties(String groupId) {
        properties = new Properties();
        properties.setProperty("bootstrap.servers", config.getString("kafka.bootstrap.servers"));
        properties.setProperty("zookeeper.connect", config.getString("kafka.zookeeper.connect"));
        properties.setProperty("group.id", groupId);
        return properties;
    }

}
