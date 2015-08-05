package org.dna.mqtt.moquette.messaging.spi.impl;

import org.dna.mqtt.moquette.proto.messages.AbstractMessage;

public class MQMessage{
	private AbstractMessage message;
	private String clientId;
	public MQMessage(){}
	public MQMessage(String cId,AbstractMessage msg){
		clientId = cId;
		message = msg;
	}
	public AbstractMessage getMessage() {
		return message;
	}
	public String getClientId() {
		return clientId;
	}
	public void setClientId(String clientId) {
		this.clientId = clientId;
	}
	public void setMessage(AbstractMessage message) {
		this.message = message;
	}
}
