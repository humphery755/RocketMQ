package com.alibaba.rocketmq.storm.domain;

import java.util.TreeMap;

import com.alibaba.rocketmq.common.message.MessageExt;
import com.google.common.collect.Maps;

/**
 * @author Von Gosling
 */
public class RocketMQCachedQueue {

    private final TreeMap<Long, MessageExt> msgCachedTable = Maps.newTreeMap();

    public TreeMap<Long, MessageExt> getMsgCachedTable() {
        return msgCachedTable;
    }
}
