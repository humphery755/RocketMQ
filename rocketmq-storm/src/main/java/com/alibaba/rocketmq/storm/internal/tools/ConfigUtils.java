package com.alibaba.rocketmq.storm.internal.tools;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.lang.BooleanUtils;

import backtype.storm.Config;

import com.alibaba.rocketmq.storm.domain.RocketMQConfig;

/**
 * Utilities for RocketMQ spout regarding its configuration and reading values
 * from the storm configuration.
 * 
 * @author Von Gosling
 */
public abstract class ConfigUtils {
    /**
     * Storm configuration key pointing to a file containing rocketmq
     * configuration ({@code "rocketmq.config"}).
     */
    public static final String CONFIG_FILE                   = "rocketmq.config";
    /**
     * Storm configuration key used to determine the rocketmq topic to read from
     * ( {@code "rocketmq.spout.topic"}).
     */
    public static final String CONFIG_TOPIC                  = "rocketmq.spout.topic";
    /**
     * Default rocketmq topic to read from ({@code "rocketmq_spout_topic"}).
     */
    public static final String CONFIG_DEFAULT_TOPIC          = "rocketmq_spout_topic";
    /**
     * Storm configuration key used to determine the rocketmq consumer group (
     * {@code "rocketmq.spout.consumer.group"}).
     */
    public static final String CONFIG_CONSUMER_GROUP         = "rocketmq.spout.consumer.group";
    /**
     * Default rocketmq consumer group id (
     * {@code "rocketmq_spout_consumer_group"}).
     */
    public static final String CONFIG_DEFAULT_CONSUMER_GROUP = "rocketmq_spout_consumer_group";
    /**
     * Storm configuration key used to determine the rocketmq topic tag(
     * {@code "rocketmq.spout.topic.tag"}).
     */
    public static final String CONFIG_TOPIC_TAG              = "rocketmq.spout.topic.tag";

    public static final String CONFIG_ROCKETMQ               = "rocketmq.config";

    /**
     * Reads configuration from a classpath resource stream obtained from the
     * current thread's class loader through
     * {@link ClassLoader#getSystemResourceAsStream(String)}.
     * 
     * @param resource The resource to be read.
     * @return A {@link java.util.Properties} object read from the specified
     *         resource.
     * @throws IllegalArgumentException When the configuration file could not be
     *             found or another I/O error occurs.
     */
    public static Properties getResource(final String resource) {
        InputStream input = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream(resource);
        if (input == null) {
            throw new IllegalArgumentException("configuration file '" + resource
                    + "' not found on classpath");
        }

        final Properties config = new Properties();
        try {
            config.load(input);
        } catch (final IOException e) {
            throw new IllegalArgumentException("reading configuration from '" + resource
                    + "' failed", e);
        }
        return config;
    }

    public static Config init(String configFile) {
        Config config = new Config();
        //1.
        Properties prop = ConfigUtils.getResource(configFile);
        for (Entry<Object, Object> entry : prop.entrySet()) {
            config.put((String) entry.getKey(), entry.getValue());
        }
        //2.
        String topic = (String) config.get(ConfigUtils.CONFIG_TOPIC);
        String consumerGroup = (String) config.get(ConfigUtils.CONFIG_CONSUMER_GROUP);
        String topicTag = (String) config.get(ConfigUtils.CONFIG_TOPIC_TAG);
        RocketMQConfig mqConfig = new RocketMQConfig(consumerGroup, topic, topicTag);
        mqConfig.setBrokerSuspendMaxTimeMillis(Long.valueOf((String)config.get("rocketmq.spout.brokerSuspendMaxTimeMillis")));
        mqConfig.setConsumerTimeoutMillisWhenSuspend(Long.valueOf((String)config.get("rocketmq.spout.consumerTimeoutMillisWhenSuspend")));

        boolean ordered = BooleanUtils.toBooleanDefaultIfNull(
                Boolean.valueOf((String) (config.get("rocketmq.spout.ordered"))), false);
        mqConfig.setOrdered(ordered);

        config.put(CONFIG_ROCKETMQ, mqConfig);
        return config;
    }

}
