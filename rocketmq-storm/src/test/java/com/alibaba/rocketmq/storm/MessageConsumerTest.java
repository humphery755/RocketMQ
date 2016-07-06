package com.alibaba.rocketmq.storm;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListener;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerOrderly;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.storm.domain.RocketMQConfig;

/**
 * @author Von Gosling
 */
public class MessageConsumerTest {

    private RocketMQConfig  config;
    private MessageListener listener;
    private MessageConsumer consumer;

    @Before
    public void init() throws Exception {
        config = new RocketMQConfig();
        config.setInstanceName("rocketmq");
        config.setTopic("rocketmq-topic");
        config.setTopicTag("rocketmq-topic-tag");
        config.setGroupId("rocketmq-group");

        listener = new MessageListenerOrderly() {

            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs,
                                                       ConsumeOrderlyContext context) {
                return ConsumeOrderlyStatus.SUCCESS;
            }
        };
        consumer = new MessageConsumer(config);
        consumer.start(listener);
    }

    @After
    public void stop() {
        consumer.shutdown();
    }

    @Test
    public void testSuspend() throws Exception {
        consumer.suspend();
    }

    @Test
    public void testResume() throws Exception {
        consumer.resume();
    }

}
