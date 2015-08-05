/*
 * Copyright (c) 2012-2014 The original author or authors
 * ------------------------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 * The Eclipse Public License is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * The Apache License v2.0 is available at
 * http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */
package org.dna.mqtt.moquette.parser.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import static org.dna.mqtt.moquette.parser.netty.TestUtils.mockChannelHandler;
import org.dna.mqtt.moquette.proto.messages.MessageType;
import org.dna.mqtt.moquette.proto.messages.SubAckMessage;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author andrea
 */
public class SubAckEncoderTest {
    SubAckEncoder m_encoder = new SubAckEncoder();
    ChannelHandlerContext m_mockedContext;
    ByteBuf m_out;
         
    @Before
    public void setUp() {
        //mock the ChannelHandlerContext to return an UnpooledAllocator
        m_mockedContext = mockChannelHandler();
        m_out = Unpooled.buffer();
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testEncodeWithNoQoss() throws Exception {
        SubAckMessage msg = new SubAckMessage();
        msg.setMessageID(123);

        //Exercise
        m_encoder.encode(m_mockedContext, msg, m_out);
    }
    
    @Test
    public void testEncodeWithMultipleQos() throws Exception {
        SubAckMessage msg = new SubAckMessage();

        int messageID = 0xAABB;
        msg.setMessageID(messageID);
        msg.addType(MessageType.QOS_MOST_ONE);
        msg.addType(MessageType.QOS_LEAST_ONE);
        msg.addType(MessageType.QOS_EXACTLY_ONCE);
        
        //Exercise
        m_encoder.encode(m_mockedContext, msg, m_out);

        //Verify
        assertEquals((byte) (MessageType.SUBACK << 4 ), m_out.readByte()); //1 byte
        assertEquals(5, m_out.readByte()); //remaining length

        //Variable part
        assertEquals((byte)0xAA, m_out.readByte()); //MessageID MSB
        assertEquals((byte)0xBB, m_out.readByte()); //MessageID LSB
        assertEquals((byte)MessageType.QOS_MOST_ONE, m_out.readByte());
        assertEquals((byte)MessageType.QOS_LEAST_ONE, m_out.readByte());
        assertEquals((byte)MessageType.QOS_EXACTLY_ONCE, m_out.readByte());
    }
}
