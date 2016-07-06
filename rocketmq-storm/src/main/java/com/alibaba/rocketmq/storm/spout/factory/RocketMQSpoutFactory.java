package com.alibaba.rocketmq.storm.spout.factory;

import java.util.ServiceLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.topology.IRichSpout;

import com.alibaba.rocketmq.storm.annotation.Extension;
import com.alibaba.rocketmq.storm.domain.RocketMQSpouts;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * @author Von Gosling
 */
public final class RocketMQSpoutFactory {
    private static final Logger              logger         = LoggerFactory
                                                                    .getLogger(RocketMQSpoutFactory.class);

    private static IRichSpout                spout;

    private static Cache<String, IRichSpout> cache          = CacheBuilder.newBuilder().build();

    private static final String              DEFAULT_BROKER = RocketMQSpouts.STREAM.getValue();

    public static IRichSpout getSpout(String spoutName) {
        RocketMQSpouts spoutType = RocketMQSpouts.fromString(spoutName);
        switch (spoutType) {
            case SIMPLE:
            case BATCH:
            case STREAM:
                return locateSpout(spoutName);
            default:
                logger.warn("Can not support this spout type {} temporarily!", spoutName);
                return locateSpout(DEFAULT_BROKER);

        }
    }

    private static IRichSpout locateSpout(String spoutName) {
        spout = cache.getIfPresent(spoutName);
        if (null == spout) {
            for (IRichSpout spoutInstance : ServiceLoader.load(IRichSpout.class)) {
                Extension ext = spoutInstance.getClass().getAnnotation(Extension.class);
                if (spoutName.equals(ext.value())) {
                    spout = spoutInstance;
                    cache.put(spoutName, spoutInstance);
                    return spoutInstance;
                }
            }
        }

        return spout;
    }

}
