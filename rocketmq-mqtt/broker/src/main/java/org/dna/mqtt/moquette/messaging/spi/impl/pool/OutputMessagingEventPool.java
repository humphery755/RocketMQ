package org.dna.mqtt.moquette.messaging.spi.impl.pool;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.dna.mqtt.moquette.messaging.spi.impl.events.OutputMessagingEvent;

public class OutputMessagingEventPool extends BasePooledObjectFactory<OutputMessagingEvent>{

	@Override
	public OutputMessagingEvent create() throws Exception {
		// TODO Auto-generated method stub
		return null;//new OutputMessagingEvent();
	}

	@Override
	public PooledObject<OutputMessagingEvent> wrap(OutputMessagingEvent arg0) {
		// TODO Auto-generated method stub
		return null;
	}

}
