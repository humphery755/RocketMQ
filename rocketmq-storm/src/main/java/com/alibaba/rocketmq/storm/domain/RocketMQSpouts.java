package com.alibaba.rocketmq.storm.domain;

import java.util.Map;

import com.google.common.collect.Maps;

/**
 * @author Von Gosling
 */
public enum RocketMQSpouts {
    SIMPLE("simple"),
    BATCH("batch"),
    STREAM("stream");

    private String value;

    RocketMQSpouts(String value) {
        this.value = value;
    }

    public String getValue() {
        return this.value;
    }

    private static Map<String, RocketMQSpouts> stringToEnum = Maps.newHashMap();

    static {
        for (RocketMQSpouts spout : values()) {
            stringToEnum.put(spout.getValue(), spout);
        }
    }

    public static RocketMQSpouts fromString(String value) {
        return stringToEnum.get(value);
    }
}
