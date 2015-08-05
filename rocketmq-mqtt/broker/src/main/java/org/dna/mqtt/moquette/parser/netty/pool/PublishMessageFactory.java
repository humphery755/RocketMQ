package org.dna.mqtt.moquette.parser.netty.pool;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.dna.mqtt.moquette.proto.messages.PublishMessage;

public class PublishMessageFactory extends BasePooledObjectFactory<PublishMessage>{
	private final static PublishMessageFactory ins=new PublishMessageFactory();
	
	public static  PublishMessageFactory getInstance(){
		return ins;
	}
	@Override
	public PublishMessage create() throws Exception {
		return new PublishMessage();
	}

	@Override
	public PooledObject<PublishMessage> wrap(PublishMessage arg0) {
		return new DefaultPooledObject<PublishMessage>(arg0);
	}
	
	@Override
    public void passivateObject(PooledObject<PublishMessage> pooledObject) {
		PublishMessage pm = pooledObject.getObject();
		pm.setDupFlag(false);
		pm.setMessageID(null);
		pm.setMessageType((byte)0);
		pm.setTopicName(null);
		pm.getPayload().clear();
    }
}
