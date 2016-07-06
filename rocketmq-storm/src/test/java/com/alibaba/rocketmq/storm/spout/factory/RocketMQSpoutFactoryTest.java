package com.alibaba.rocketmq.storm.spout.factory;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.core.Is.isA;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import backtype.storm.topology.IRichSpout;

import com.alibaba.rocketmq.storm.domain.RocketMQSpouts;
import com.alibaba.rocketmq.storm.spout.BatchMessageSpout;
import com.alibaba.rocketmq.storm.spout.SimpleMessageSpout;
import com.alibaba.rocketmq.storm.spout.StreamMessageSpout;

/**
 * @author Von Gosling
 */
public class RocketMQSpoutFactoryTest {

    @Test(expected = java.lang.NullPointerException.class)
    public void getNullSpoutTest() throws Exception {
        RocketMQSpoutFactory.getSpout(null);
    }

    @Test
    public void getSpoutTest() throws Exception {
        IRichSpout richSpout = RocketMQSpoutFactory.getSpout(RocketMQSpouts.SIMPLE.getValue());
        assertThat(richSpout, isA(IRichSpout.class));
        assertThat(richSpout, instanceOf(SimpleMessageSpout.class));

        richSpout = RocketMQSpoutFactory.getSpout(RocketMQSpouts.BATCH.getValue());
        assertThat(richSpout, instanceOf(BatchMessageSpout.class));

        richSpout = RocketMQSpoutFactory.getSpout(RocketMQSpouts.STREAM.getValue());
        assertThat(richSpout, instanceOf(StreamMessageSpout.class));
    }
}
