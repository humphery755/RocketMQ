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
package org.dna.mqtt.moquette.server.netty;

import static org.dna.mqtt.moquette.proto.messages.MessageType.CONNECT;
import static org.dna.mqtt.moquette.proto.messages.MessageType.DISCONNECT;
import static org.dna.mqtt.moquette.proto.messages.MessageType.PINGREQ;
import static org.dna.mqtt.moquette.proto.messages.MessageType.PUBACK;
import static org.dna.mqtt.moquette.proto.messages.MessageType.PUBCOMP;
import static org.dna.mqtt.moquette.proto.messages.MessageType.PUBLISH;
import static org.dna.mqtt.moquette.proto.messages.MessageType.PUBREC;
import static org.dna.mqtt.moquette.proto.messages.MessageType.PUBREL;
import static org.dna.mqtt.moquette.proto.messages.MessageType.SUBSCRIBE;
import static org.dna.mqtt.moquette.proto.messages.MessageType.UNSUBSCRIBE;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.dna.mqtt.moquette.messaging.spi.IMessaging;
import org.dna.mqtt.moquette.proto.Utils;
import org.dna.mqtt.moquette.proto.messages.AbstractMessage;
import org.dna.mqtt.moquette.proto.messages.PingRespMessage;
import org.dna.mqtt.moquette.server.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author andrea
 */
@Sharable
public class NettyMQTTHandler extends SimpleChannelInboundHandler {
    
    private static final Logger LOG = LoggerFactory.getLogger(NettyMQTTHandler.class);
    private IMessaging m_messaging;
    private final ConcurrentHashMap<ChannelHandlerContext, NettyChannel> m_channelMapper = new ConcurrentHashMap<ChannelHandlerContext, NettyChannel>();
    
    @Override
    public void channelRead0(ChannelHandlerContext ctx, Object message) {
        AbstractMessage msg = (AbstractMessage) message;
        LOG.info("Received a message of type {}", Utils.msgType2String(msg.getMessageType()));
        try {
            switch (msg.getMessageType()) {
            	case SUBSCRIBE:
            		LOG.info("Received a message of type {}", Utils.msgType2String(msg.getMessageType()));
                case CONNECT:
                case UNSUBSCRIBE:
                case PUBLISH:
                case PUBREC:
                case PUBCOMP:
                case PUBREL:
                case DISCONNECT:
                case PUBACK:    
                    NettyChannel channel = m_channelMapper.get(ctx);
                    if(channel==null){
                    	NettyChannel newChannel = new NettyChannel(ctx);
                    	channel=m_channelMapper.putIfAbsent(ctx, newChannel);
                    	if(channel==null)channel=newChannel;
                    }
                    
                    m_messaging.handleProtocolMessage(channel, msg);
                    break;
                case PINGREQ:
                    PingRespMessage pingResp = new PingRespMessage();
                    ctx.writeAndFlush(pingResp);
                    break;
            }
        } catch (Exception ex) {
            LOG.error("Bad error in processing the message", ex);
        }
    }
    
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        NettyChannel channel = m_channelMapper.get(ctx);
        if(channel==null){
        	LOG.error("{} is null",ctx);
        	ctx.close(/*false*/);
        	return;
        }
        
        String clientID = (String) channel.getAttribute(Constants.ATTR_CLIENTID);
        if(clientID!=null)
        	m_messaging.lostConnection(clientID);
        else
        	LOG.error("{} is null",Constants.ATTR_CLIENTID);
        ctx.close(/*false*/);
        m_channelMapper.remove(ctx);
    }
    
    public void setMessaging(IMessaging messaging) {
        m_messaging = messaging;
    }
    

}
