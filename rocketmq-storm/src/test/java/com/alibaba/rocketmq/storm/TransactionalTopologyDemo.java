package com.alibaba.rocketmq.storm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.storm.domain.RocketMQConfig;
import com.alibaba.rocketmq.storm.internal.tools.ConfigUtils;
import com.alibaba.rocketmq.storm.trident.RocketMQTridentSpout;

/**
 * @author Von Gosling
 */
public class TransactionalTopologyDemo {

    public static final Logger  LOG            = LoggerFactory
                                                       .getLogger(TransactionalTopologyDemo.class);

    private static final String PROP_FILE_NAME = "mqspout.test.prop";

    public static StormTopology buildTopology() throws MQClientException {
        TridentTopology topology = new TridentTopology();

        Config config = ConfigUtils.init(PROP_FILE_NAME);
        RocketMQConfig rocketMQConfig = (RocketMQConfig) config.get(ConfigUtils.CONFIG_ROCKETMQ);

        RocketMQTridentSpout spout = new RocketMQTridentSpout(rocketMQConfig);
        Stream stream = topology.newStream("rocketmq-txId", spout);
        stream.each(new Fields("message"), new BaseFilter() {
            private static final long serialVersionUID = -9056745088794551960L;

            @Override
            public boolean isKeep(TridentTuple tuple) {
                LOG.debug("Entering filter...");
                return true;
            }

        });
        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(String.valueOf(conf.get("topology.name")), conf, buildTopology());

        Thread.sleep(50000);

        cluster.shutdown();
    }
}
