package com.alibaba.rocketmq.storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author Von Gosling
 */
public class RocketMqBolt implements IRichBolt {
    private static final long   serialVersionUID = 7591260982890048043L;

    private static final Logger LOG              = LoggerFactory.getLogger(RocketMqBolt.class);

    private OutputCollector     collector;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {
        Object msgObj = input.getValue(0);
        Object msgStat = input.getValue(1);
        try {
            LOG.info("Messages:" + msgObj + "\n statistics:" + msgStat);

        } catch (Exception e) {
            collector.fail(input);
            return;
            //throw new FailedException(e);
        }
        collector.ack(input);
    }

    public void cleanup() {
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
