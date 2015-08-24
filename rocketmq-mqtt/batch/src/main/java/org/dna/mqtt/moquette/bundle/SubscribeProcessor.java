package org.dna.mqtt.moquette.bundle;

import io.netty.buffer.ByteBuf;

import org.apache.commons.lang3.StringUtils;
import org.dna.mqtt.commons.ObjectUtils;
import org.dna.mqtt.moquette.parser.netty.Utils;
import org.dna.mqtt.moquette.proto.messages.MessageType;
import org.nutz.ssdb4j.spi.Response;
import org.nutz.ssdb4j.spi.SSDB;

import com.alibaba.fastjson.JSON;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.common.message.Message;

public class SubscribeProcessor implements MqttProcessor {
	private MQTTUPConsumer mqttConsumer;
	private SSDB ssdb;
	private GroupNotifyService groupNotifyService;
	private DefaultMQProducer mqProducer;
	
	public SubscribeProcessor(MQTTUPConsumer mqttConsumer) {
		this.mqttConsumer = mqttConsumer;
		this.ssdb = mqttConsumer.getSsdb();
		this.groupNotifyService = mqttConsumer.getGroupNotifyService();
		mqProducer = mqttConsumer.getCustomEvtMqProducer();
		init();
	}
	
	private void init(){
		
	}

	public boolean process(ByteBuf in, int startPos, byte messageTypeFlags) throws Exception {
		byte messageType = (byte) ((messageTypeFlags & 0x00F0) >> 4);
		byte flags = (byte) (messageTypeFlags & 0x0F);
		/*
		 * if (expectedFlagsOpt != null) { int expectedFlags = expectedFlagsOpt;
		 * if ((byte) expectedFlags != flags) { String hexExpected =
		 * Integer.toHexString(expectedFlags); String hexReceived =
		 * Integer.toHexString(flags); throw new
		 * CorruptedFrameException(String.format
		 * ("Received a message with fixed header flags (%s) != expected (%s)",
		 * hexReceived, hexExpected)); } }
		 */

		boolean dupFlag = ((byte) ((messageTypeFlags & 0x0008) >> 3) == 1);
		byte qosLevel = (byte) ((messageTypeFlags & 0x0006) >> 1);
		boolean retainFlag = ((byte) (messageTypeFlags & 0x0001) == 1);
		int remainingLength = Utils.decodeRemainingLenght(in);
		if (remainingLength == -1) {
			return false;
		}
		String clientId = Utils.decodeString(in);
		if (clientId == null) {
			in.resetReaderIndex();
			return false;
		}
		String topic = Utils.decodeString(in);
		if (topic == null) {
			in.resetReaderIndex();
			return false;
		}
		if (!StringUtils.startsWith(topic, "G/CUSTOM/")) {
			in.resetReaderIndex();
			Response resp = ssdb.hincr("MQTT_MSG_SEQUENCE", topic, 1);
			if (resp.ok()) {
				long msgOffset = resp.asLong();// mqttConsumer.incByTopic(topic);
				ssdb.hset(topic, msgOffset, in.array());
				ssdb.zset(topic, msgOffset,msgOffset);
				groupNotifyService.putRequest(topic, msgOffset);
			}

			return true;
		}
		
		// 后续分析扩展用
		int msgId = -1;
		if (qosLevel == MessageType.QOS_LEAST_ONE || qosLevel == MessageType.QOS_EXACTLY_ONCE) {
			msgId = in.readUnsignedShort();
		}

		int stopPos = in.readerIndex();

		// read the payload
		int payloadSize = remainingLength - (stopPos - startPos - 2) + (Utils.numBytesToEncode(remainingLength) - 1);
		if (in.readableBytes() < payloadSize) {
			in.resetReaderIndex();
			return false;
		}
		byte[] payload = new byte[payloadSize];
/*		ByteBuf payload = Unpooled.buffer(payloadSize);*/
		in.readBytes(payload);
		PublishBean bean=new PublishBean();
		bean.setClientId(clientId);
		bean.setMsgId(msgId);
		bean.setPayload(payload);
		bean.setQosLevel(qosLevel);
		bean.setTopic(topic);
		Message msg = new Message("MQTT-UP-PUB-CUSTOM", topic, msgId==-1?null:String.valueOf(msgId),JSON.toJSONString(bean).getBytes());
		mqProducer.sendOneway(msg);
		return true;
	}
	public class PublishBean{
		byte qosLevel;
		String clientId;
		String topic;
		int msgId;
		byte[] payload;
		public byte getQosLevel() {
			return qosLevel;
		}
		public void setQosLevel(byte qosLevel) {
			this.qosLevel = qosLevel;
		}
		public String getClientId() {
			return clientId;
		}
		public void setClientId(String clientId) {
			this.clientId = clientId;
		}
		public String getTopic() {
			return topic;
		}
		public void setTopic(String topic) {
			this.topic = topic;
		}
		public int getMsgId() {
			return msgId;
		}
		public void setMsgId(int msgId) {
			this.msgId = msgId;
		}
		public byte[] getPayload() {
			return payload;
		}
		public void setPayload(byte[] payload) {
			this.payload = payload;
		}
	}
	public static int decodeRemainingLenght(ByteBuf in) {
		int multiplier = 1;
		int value = 0;
		byte digit;
		do {
			if (in.readableBytes() < 1) {
				return -1;
			}
			digit = in.readByte();
			value += (digit & 0x7F) * multiplier;
			multiplier *= 128;
		} while ((digit & 0x80) != 0);
		return value;
	}
}
