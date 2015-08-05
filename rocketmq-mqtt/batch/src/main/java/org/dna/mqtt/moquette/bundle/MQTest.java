package org.dna.mqtt.moquette.bundle;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.dna.mqtt.moquette.parser.netty.Utils;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.MQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;

public class MQTest {
	Map<Integer,MQPushConsumer> consumerMap = new ConcurrentHashMap();
	private void testMq(int group){
		DefaultMQPushConsumer mqConsumer = new DefaultMQPushConsumer("MQTT_UP_CG"+group);
		try {
			mqConsumer.setConsumeThreadMax(1);
			mqConsumer.setConsumeThreadMin(1);
			//mqConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
			mqConsumer.subscribe("MQTT_TUP_G1", "*");
			mqConsumer.registerMessageListener(new MessageListenerConcurrently() {

				public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {

					for (MessageExt me : msgs) {
						me.getTags();
						//System.out.println(Thread.currentThread().getName() + " Receive New Messages: " + me.getTopic() + " " + me);
					}
					// return ConsumeConcurrentlyStatus.RECONSUME_LATER;
					return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
				}

			});
			mqConsumer.start();

		} catch (MQClientException e) {
			e.printStackTrace();
			System.exit(1);
		}
		consumerMap.put(group, mqConsumer);
	}
	static ByteBuf encodeString(String str) {
        ByteBuf out = Unpooled.buffer(2);
        byte[] raw;
        try {
            raw = str.getBytes("UTF-8");
            //NB every Java platform has got UTF-8 encoding by default, so this 
            //exception are never raised.
        } catch (UnsupportedEncodingException ex) {
            LoggerFactory.getLogger(Utils.class).error(null, ex);
            return null;
        }
        //Utils.writeWord(out, raw.length);
        out.writeShort(raw.length);
        out.writeBytes(raw);
        return out;
    }
	
	public static void main(String[] args) throws Exception{
		encodeString("MQIsdp");
		long time = System.currentTimeMillis();
		MQTest self = new MQTest();
    	for(int i=1;i>0;i--){
    		self.testMq(i);
    	}
    	for(Map.Entry<Integer, MQPushConsumer> entry:self.consumerMap.entrySet()){
    		
    	}
    	System.out.println(Thread.currentThread().getName() + "  "+(System.currentTimeMillis() - time)+"ms");
    }
}
