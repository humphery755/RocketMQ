package org.dna.mqtt.moquette.parser.netty.pool;

import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.dna.mqtt.moquette.proto.messages.PublishMessage;

public class PoolUtils {
	private final static ObjectPool<PublishMessage> pmPool = new GenericObjectPool<PublishMessage>(new PublishMessageFactory());
	
	public static ObjectPool<PublishMessage> getPublishMsgPool(){
		return pmPool;
	}
	
}
