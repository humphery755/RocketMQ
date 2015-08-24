package org.dna.mqtt.moquette.bundle;

import io.netty.buffer.ByteBuf;

public interface MqttProcessor {
	public boolean process(ByteBuf in, int startPos, byte messageTypeFlags) throws Exception;
}
