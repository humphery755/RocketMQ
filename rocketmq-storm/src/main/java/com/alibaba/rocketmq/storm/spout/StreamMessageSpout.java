package com.alibaba.rocketmq.storm.spout;

import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.RotatingMap;
import backtype.storm.utils.RotatingMap.ExpiredCallback;

import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.storm.annotation.Extension;
import com.alibaba.rocketmq.storm.domain.BatchMessage;
import com.alibaba.rocketmq.storm.domain.MessageCacheItem;
import com.google.common.collect.Sets;

/**
 * @author Von Gosling
 */
@Extension("stream")
public class StreamMessageSpout extends BatchMessageSpout {
    private static final long                     serialVersionUID = 464153253576782163L;

    private static final Logger                   LOG              = LoggerFactory
                                                                           .getLogger(StreamMessageSpout.class);

    private final Queue<MessageCacheItem>         msgQueue         = new ConcurrentLinkedQueue<MessageCacheItem>();
    private RotatingMap<String, MessageCacheItem> msgCache;

    /**
     * This field is used to check whether one batch is finish or not
     */
    private Map<UUID, BatchMsgsTag>               batchMsgsMap     = new ConcurrentHashMap<UUID, BatchMsgsTag>();

    public void open(@SuppressWarnings("rawtypes") final Map conf, final TopologyContext context,
                     final SpoutOutputCollector collector) {
        super.open(conf, context, collector);
        ExpiredCallback<String, MessageCacheItem> callback = new ExpiredCallback<String, MessageCacheItem>() {
            public void expire(String key, MessageCacheItem val) {
                LOG.warn("Long time no ack,key is {},value is {}", key, val);
                msgCache.put(key, val);
                fail(key);
            }

        };
        msgCache = new RotatingMap<String, MessageCacheItem>(3600 * 5, callback);

        LOG.info("Topology {} opened {} spout successfully!",
                new Object[] { topologyName, config.getTopic() });
    }

    public void prepareMsg() {
        while (true) {
            BatchMessage msgTuple = null;
            try {
                msgTuple = super.getBatchQueue().poll(1, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                return;
            }
            if (msgTuple == null) {
                return;
            }

            if (msgTuple.getMsgList().size() == 0) {
                super.finish(msgTuple.getBatchId());
                return;
            }

            BatchMsgsTag partTag = new BatchMsgsTag();
            Set<String> msgIds = partTag.getMsgIds();
            for (MessageExt msg : msgTuple.getMsgList()) {
                String msgId = msg.getMsgId();
                msgIds.add(msgId);
                MessageCacheItem item = new MessageCacheItem(msgTuple.getBatchId(), msg,
                        msgTuple.getMessageStat());
                msgCache.put(msgId, item);
                msgQueue.offer(item);
            }
            batchMsgsMap.put(msgTuple.getBatchId(), partTag);
        }
    }

    @Override
    public void nextTuple() {
        MessageCacheItem cacheItem = msgQueue.poll();
        if (cacheItem != null) {
            Values values = new Values(cacheItem.getMsg(), cacheItem.getMsgStat());
            String messageId = cacheItem.getMsg().getMsgId();
            collector.emit(values, messageId);

            LOG.debug("Emited tuple {},mssageId is {}!", values, messageId);
            return;
        }

        prepareMsg();

        return;
    }

    public void finish(String msgId) {
        MessageCacheItem cacheItem = (MessageCacheItem) msgCache.remove(msgId);
        if (cacheItem == null) {
            LOG.warn("Failed to get cached values {}!", msgId);
            return;
        }

        UUID batchId = cacheItem.getId();
        BatchMsgsTag partTag = batchMsgsMap.get(batchId);
        if (partTag == null) {
            throw new RuntimeException("In partOffset map, no entry of " + batchId);
        }

        Set<String> msgIds = partTag.getMsgIds();

        msgIds.remove(msgId);

        if (msgIds.size() == 0) {
            batchMsgsMap.remove(batchId);
            super.finish(batchId);
        }

    }

    @Override
    public void ack(final Object id) {
        if (id instanceof String) {
            finish((String) id);
            return;
        } else {
            LOG.error("Id isn't Long, type is {}!", id.getClass().getName());
        }
    }

    public void handleFail(String msgId) {
        MessageCacheItem cacheItem = msgCache.get(msgId);
        if (cacheItem == null) {
            LOG.warn("Failed to get cached values {}!", msgId);
            return;
        }

        LOG.info("Fail to handle {}!", cacheItem);

        int failTime = cacheItem.getMsgStat().getFailureTimes().incrementAndGet();
        if (config.getMaxFailTimes() < 0 || failTime < config.getMaxFailTimes()) {
            msgQueue.offer(cacheItem);
            return;
        } else {
            LOG.info("Skip message {}!", cacheItem.getMsg().toString());
            finish(msgId);
            return;
        }

    }

    @Override
    public void fail(final Object id) {
        if (id instanceof String) {
            handleFail((String) id);
            return;
        } else {
            LOG.error("Id isn't Long, type is {}" + id.getClass().getName());
        }
    }

    public void declareOutputFields(final OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("MessageExt"));
    }

    public static class BatchMsgsTag {
        private final Set<String> msgIds;
        private final long        createTs;

        public BatchMsgsTag() {
            this.msgIds = Sets.newHashSet();
            this.createTs = System.currentTimeMillis();
        }

        public Set<String> getMsgIds() {
            return msgIds;
        }

        public long getCreateTs() {
            return createTs;
        }

        @Override
        public String toString() {
            return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
        }
    }

}
