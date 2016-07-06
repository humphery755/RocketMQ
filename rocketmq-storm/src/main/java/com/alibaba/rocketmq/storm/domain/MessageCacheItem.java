package com.alibaba.rocketmq.storm.domain;

import com.alibaba.rocketmq.common.message.MessageExt;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import java.util.UUID;

/**
 * @author Von Gosling
 */
public class MessageCacheItem {
    private final UUID        id;
    private final MessageExt  msg;
    private final MessageStat msgStat;

    public MessageCacheItem(UUID id, MessageExt msg, MessageStat msgStat) {
        this.id = id;
        this.msg = msg;
        this.msgStat = msgStat;
    }

    public UUID getId() {
        return id;
    }

    public MessageExt getMsg() {
        return msg;
    }

    public MessageStat getMsgStat() {
        return msgStat;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }

}
