package org.dna.mqtt.moquette.bundle;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.dna.mqtt.commons.ObjectUtils;
import org.dna.mqtt.moquette.messaging.spi.impl.MQMessage;
import org.dna.mqtt.moquette.messaging.spi.impl.PublishMessageExt;
import org.dna.mqtt.moquette.proto.messages.MessageType;
import org.dna.mqtt.moquette.proto.messages.PubCompMessage;
import org.dna.mqtt.moquette.proto.messages.PubRelMessage;
import org.dna.mqtt.moquette.server.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.taobao.tair.DataEntry;
import com.taobao.tair.Result;
import com.taobao.tair.ResultCode;
import com.taobao.tair.TairManager;
import com.taobao.tair.impl.DefaultTairManager;

public class MQUPConsumer {
	private static final Logger LOG = LoggerFactory.getLogger(MQUPConsumer.class);
	private Integer mygid;
	private DefaultMQPushConsumer mqConsumer;
	private DefaultMQProducer mqProducer;
	private TairManager tairManager;

	void init(Properties configProps) {
		mygid = Integer.valueOf(configProps.getProperty("mygid"));
		DefaultTairManager _tairManager = new DefaultTairManager();
		List confServList = new ArrayList();
		String[] servList = StringUtils.split(configProps.getProperty("tair.confServList"), ";");
		for (String serv : servList)
			confServList.add(serv);
		_tairManager.setConfigServerList(confServList);
		_tairManager.setGroupName(configProps.getProperty("tair.group"));

		mqProducer = new DefaultMQProducer(String.format("MQTT_DOWN_PG%d",mygid));
		mqConsumer = new DefaultMQPushConsumer(String.format("MQTT_UP_CG%d",mygid));
		
		final String CURRENT_TOPIC_UP = String.format(Constants.TOPIC_UPG_PATTERN,mygid);
		final String CURRENT_TOPIC_DOWN = String.format(Constants.TOPIC_DOWNG_PATTERN,mygid);
		try {
			_tairManager.init();
			tairManager = _tairManager;
			mqProducer.start();
			mqConsumer.subscribe(CURRENT_TOPIC_UP, "*");
			mqConsumer.registerMessageListener(new MessageListenerConcurrently() {

				public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
					//if(true)return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
					for (MessageExt me : msgs) {
						MQMessage mqMessage = ObjectUtils.deserialize(me.getBody(), MQMessage.class);
						String srcClientId = mqMessage.getClientId();
						try {
							if (mqMessage.getMessage() instanceof PublishMessageExt) {
								PublishMessageExt m = (PublishMessageExt) mqMessage.getMessage();
								if (me.getTopic().equals(m.getTopicName()))
									continue;
								LOG.trace("SRV --PUBLISH--> BATCH consumeMessage invoked for clientID {} with messageID {} with tag {} ",
										srcClientId, m.getMessageID(), me.getTags());
								Integer messageID = m.getMessageID();
								String publishKey = String.format("%s%d", srcClientId, messageID);
								int hashCode = m.getTopicName().hashCode();
								if (hashCode < 0)
									hashCode = -hashCode;

								Message msg = new Message(CURRENT_TOPIC_DOWN, m.getTopicName(), publishKey, ObjectUtils.serialize(mqMessage));
								if (m.getQos() == MessageType.QOS_MOST_ONE){
									mqProducer.sendOneway(msg);
								}else{
									SendResult sendResult = mqProducer.send(msg);
									switch (sendResult.getSendStatus()) {
									case SEND_OK:
									case SLAVE_NOT_AVAILABLE:
										break;
									default:
										return ConsumeConcurrentlyStatus.RECONSUME_LATER;
									}
								}
							} else if (mqMessage.getMessage() instanceof PubRelMessage) {
								PubRelMessage m = (PubRelMessage) mqMessage.getMessage();
								LOG.trace("SRV --PUBREL--> BATCH consumeMessage invoked for clientID {} with messageID {} with tag {} ", srcClientId,
										m.getMessageID(), me.getTags());
								Integer messageID = m.getMessageID();

								String publishKey = String.format("%s%d", srcClientId, messageID);
								Result<DataEntry> res = tairManager.get(1, publishKey);
								if (res.getRc() != ResultCode.SUCCESS)
									continue;
								byte[] bPubMsg = (byte[]) res.getValue().getValue();
								MQMessage pubMessage = ObjectUtils.deserialize(bPubMsg, MQMessage.class);
								PublishMessageExt pm = (PublishMessageExt) pubMessage.getMessage();


								LOG.trace("BATCH --PUBLISH--> SRV send invoked for clientID {} with messageID {} with tag {} ", srcClientId,
										messageID, pm.getTopicName());
								Message msg = new Message(CURRENT_TOPIC_DOWN, pm.getTopicName(), publishKey, ObjectUtils.serialize(pubMessage));
								SendResult sendResult = mqProducer.send(msg);
								switch (sendResult.getSendStatus()) {
								case SEND_OK:
								case SLAVE_NOT_AVAILABLE:
									break;
								default:
									return ConsumeConcurrentlyStatus.RECONSUME_LATER;
								}

								PubCompMessage pubCompMessage = new PubCompMessage();
								pubCompMessage.setMessageID(messageID);
								mqMessage.setMessage(pubCompMessage);
								LOG.trace("BATCH --PUBREL--> SRV send invoked for clientID {} with messageID {} with tag {} ", srcClientId,
										messageID, srcClientId);
								msg = new Message(CURRENT_TOPIC_DOWN, srcClientId, publishKey, ObjectUtils.serialize(mqMessage));
								sendResult = mqProducer.send(msg);
								switch (sendResult.getSendStatus()) {
								case SEND_OK:
								case SLAVE_NOT_AVAILABLE:
									tairManager.delete(1, publishKey);
									break;
								default:
									return ConsumeConcurrentlyStatus.RECONSUME_LATER;
								}
							} else if (mqMessage.getMessage() instanceof PubCompMessage) {
								// ...
								PubCompMessage pubCompMessage = (PubCompMessage) mqMessage.getMessage();
								Integer messageID = pubCompMessage.getMessageID();
								LOG.trace("SRV --PUBCOMP--> BATCH consumeMessage invoked for clientID {} with messageID {} with tag {} ",
										srcClientId, messageID, srcClientId);

							}
						} catch (Exception e) {
							LOG.error("", e);
							return ConsumeConcurrentlyStatus.RECONSUME_LATER;
						}
					}
					// return ConsumeConcurrentlyStatus.RECONSUME_LATER;
					return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
				}

			});

			mqConsumer.start();

		} catch (Exception e) {
			LOG.error("", e);
			System.exit(1);
		}
	}

}
