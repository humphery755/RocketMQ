package com.alibaba.rocketmq.storm;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;
import mockit.Mocked;

import org.junit.Before;
import org.junit.Test;

import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.MQConsumer;
import com.alibaba.rocketmq.client.consumer.listener.MessageListener;
import com.alibaba.rocketmq.storm.domain.RocketMQConfig;

/**
 * @author Von Gosling
 */
public class MessageConsumerManagerTest {

    private RocketMQConfig  config;

    @Mocked
    private MessageListener listener;

    @Before
    public void init() {
        config = new RocketMQConfig();
        config.setInstanceName("rocketmq");
        config.setTopic("rocketmq-topic");
        config.setTopicTag("rocketmq-topic-tag");
        config.setGroupId("rocketmq-group");
    }

    @Test
    public void testGetConsumerInstance() throws Exception {
        MQConsumer consumer = MessageConsumerManager.getConsumerInstance(config, listener, true);
        assertThat(consumer, instanceOf(DefaultMQPushConsumer.class));

        consumer = MessageConsumerManager.getConsumerInstance(config, listener, false);
        assertThat(consumer, instanceOf(DefaultMQPullConsumer.class));
    }
}
