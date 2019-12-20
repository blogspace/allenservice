package com.service;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class RedisWriter implements RedisMapper<Tuple2<String, String>> {
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.HSET, "");
    }

    public String getKeyFromData(Tuple2<String, String> keyvalue) {
        return keyvalue.f0;
    }

    public String getValueFromData(Tuple2<String, String> keyvalue) {
        return keyvalue.f1;
    }
}
