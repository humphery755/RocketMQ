package org.dna.mqtt.moquette.bundle;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelProgressivePromise;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.AttributeMap;
import io.netty.util.concurrent.EventExecutor;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.dna.mqtt.moquette.parser.netty.Utils;
import org.dna.mqtt.moquette.proto.messages.AbstractMessage;
import org.dna.mqtt.moquette.proto.messages.ConnAckMessage;
import org.dna.mqtt.moquette.proto.messages.ConnectMessage;
import org.dna.mqtt.moquette.proto.messages.MessageType;
import org.dna.mqtt.moquette.proto.messages.SubAckMessage;
import org.dna.mqtt.moquette.proto.messages.SubscribeMessage;
import org.dna.mqtt.moquette.proto.messages.SubscribeMessage.Couple;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketListener;

//@WebSocket(maxBinaryMessageSize = 64 * 1024)
public class MqttSocket implements WebSocketListener {
	private final CountDownLatch closeLatch;

	@SuppressWarnings("unused")
	private Session session;

	public MqttSocket() {
		this.closeLatch = new CountDownLatch(1);
	}

	public boolean awaitClose(int duration, TimeUnit unit) throws InterruptedException {
		return this.closeLatch.await(duration, unit);
	}

	public void onWebSocketBinary(byte[] msg, int a, int b) {
		ByteBuf in = Unpooled.wrappedBuffer(msg);
		
		try {
			byte h1 = in.readByte();
			byte messageType = (byte) ((h1 & 0x00F0) >> 4);
			switch (messageType) {
			case MessageType.CONNACK:{
				List<ConnAckMessage> l = new ArrayList();
				decodeConnAck(null, in, l);
				ConnAckMessage ack = l.get(0);
				System.out.printf("Got ReturnCode: %d%n", ack.getReturnCode());
			}
			break;
			case MessageType.SUBACK:{
				List<SubAckMessage> l = new ArrayList();
				decodeSUBACK(null, in, l);
				SubAckMessage ack = l.get(0);
				System.out.printf("Got MessageID: %d%n", ack.getMessageID());
			}
				break;
			case MessageType.PUBACK:
				break;
			default:

			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	 void decodeSUBACK(AttributeMap ctx, ByteBuf in, List<SubAckMessage> out) throws Exception {
	        //Common decoding part
	        in.resetReaderIndex();
	        SubAckMessage message = new SubAckMessage();
	        if (!genericDecodeCommonHeader(message, 0x00, in)) {
	            in.resetReaderIndex();
	            return;
	        }
	        int remainingLength = message.getRemainingLength();
	        
	        //MessageID
	        message.setMessageID(in.readUnsignedShort());
	        remainingLength -= 2;
	        
	        //Qos array
	        if (in.readableBytes() < remainingLength ) {
	            in.resetReaderIndex();
	            return;
	        }
	        for (int i = 0; i < remainingLength; i++) {
	            byte qos = in.readByte();
	            message.addType(qos);
	        }
	        
	        out.add(message);
	    }
	void decodeConnAck(AttributeMap ctx, ByteBuf in, List<ConnAckMessage> out) throws Exception {
		in.resetReaderIndex();
		// Common decoding part
		ConnAckMessage message = new ConnAckMessage();
		if (!genericDecodeCommonHeader(message, 0x00, in)) {
			in.resetReaderIndex();
			return;
		}
		// skip reserved byte
		in.skipBytes(1);

		// read return code
		message.setReturnCode(in.readByte());
		out.add(message);
	}

	private boolean genericDecodeCommonHeader(AbstractMessage message, Integer expectedFlagsOpt, ByteBuf in) {
		// Common decoding part
		if (in.readableBytes() < 2) {
			return false;
		}
		byte h1 = in.readByte();
		byte messageType = (byte) ((h1 & 0x00F0) >> 4);

		byte flags = (byte) (h1 & 0x0F);
		if (expectedFlagsOpt != null) {
			int expectedFlags = expectedFlagsOpt;
			if ((byte) expectedFlags != flags) {
				String hexExpected = Integer.toHexString(expectedFlags);
				String hexReceived = Integer.toHexString(flags);
				throw new CorruptedFrameException(String.format("Received a message with fixed header flags (%s) != expected (%s)", hexReceived,
						hexExpected));
			}
		}

		boolean dupFlag = ((byte) ((h1 & 0x0008) >> 3) == 1);
		byte qosLevel = (byte) ((h1 & 0x0006) >> 1);
		boolean retainFlag = ((byte) (h1 & 0x0001) == 1);
		int remainingLength = Utils.decodeRemainingLenght(in);
		if (remainingLength == -1) {
			return false;
		}

		message.setMessageType(messageType);
		message.setDupFlag(dupFlag);
		message.setQos(qosLevel);
		message.setRetainFlag(retainFlag);
		message.setRemainingLength(remainingLength);
		return true;
	}

	protected void encode(ChannelHandlerContext chc, SubscribeMessage message, ByteBuf out) {
        if (message.subscriptions().isEmpty()) {
           throw new IllegalArgumentException("Found a subscribe message with empty topics");
       }

       if (message.getQos() != MessageType.QOS_LEAST_ONE) {
           throw new IllegalArgumentException("Expected a message with QOS 1, found " + message.getQos());
       }
       
       ByteBuf variableHeaderBuff = chc.alloc().buffer(4);
       ByteBuf buff = null;
       try {
           variableHeaderBuff.writeShort(message.getMessageID());
           for (SubscribeMessage.Couple c : message.subscriptions()) {
               variableHeaderBuff.writeBytes(Utils.encodeString(c.getTopicFilter()));
               variableHeaderBuff.writeByte(c.getQos());
           }

           int variableHeaderSize = variableHeaderBuff.readableBytes();
           byte flags = Utils.encodeFlags(message);
           buff = chc.alloc().buffer(2 + variableHeaderSize);

           buff.writeByte(MessageType.SUBSCRIBE << 4 | flags);
           buff.writeBytes(Utils.encodeRemainingLength(variableHeaderSize));
           buff.writeBytes(variableHeaderBuff);

           out.writeBytes(buff);
       } finally {
            variableHeaderBuff.release();
            buff.release();
       }
   }
	
	protected void encode(ChannelHandlerContextTest chc, ConnectMessage message, ByteBuf out) {
		ByteBuf staticHeaderBuff = chc.alloc().buffer(12);
		ByteBuf buff = chc.alloc().buffer();
		ByteBuf variableHeaderBuff = chc.alloc().buffer(12);
		try {
			staticHeaderBuff.writeBytes(Utils.encodeString("MQIsdp"));

			// version
			staticHeaderBuff.writeByte(0x03);

			// connection flags and Strings
			byte connectionFlags = 0;
			if (message.isCleanSession()) {
				connectionFlags |= 0x02;
			}
			if (message.isWillFlag()) {
				connectionFlags |= 0x04;
			}
			connectionFlags |= ((message.getWillQos() & 0x03) << 3);
			if (message.isWillRetain()) {
				connectionFlags |= 0x020;
			}
			if (message.isPasswordFlag()) {
				connectionFlags |= 0x040;
			}
			if (message.isUserFlag()) {
				connectionFlags |= 0x080;
			}
			staticHeaderBuff.writeByte(connectionFlags);

			// Keep alive timer
			staticHeaderBuff.writeShort(message.getKeepAlive());

			// Variable part
			if (message.getClientID() != null) {
				variableHeaderBuff.writeBytes(Utils.encodeString(message.getClientID()));
				if (message.isWillFlag()) {
					variableHeaderBuff.writeBytes(Utils.encodeString(message.getWillTopic()));
					variableHeaderBuff.writeBytes(Utils.encodeString(message.getWillMessage()));
				}
				if (message.isUserFlag() && message.getUsername() != null) {
					variableHeaderBuff.writeBytes(Utils.encodeString(message.getUsername()));
					if (message.isPasswordFlag() && message.getPassword() != null) {
						variableHeaderBuff.writeBytes(Utils.encodeString(message.getPassword()));
					}
				}
			}

			int variableHeaderSize = variableHeaderBuff.readableBytes();
			buff.writeByte(MessageType.CONNECT << 4);
			buff.writeBytes(Utils.encodeRemainingLength(12 + variableHeaderSize));
			buff.writeBytes(staticHeaderBuff).writeBytes(variableHeaderBuff);
			buff.resetReaderIndex();
			out.writeBytes(buff);
		} finally {
			staticHeaderBuff.release();
			buff.release();
			variableHeaderBuff.release();
		}
	}

	public void onWebSocketClose(int statusCode, String reason) {
		System.out.printf("Connection closed: %d - %s%n", statusCode, reason);
		this.session = null;
		this.closeLatch.countDown();
	}

	public void onWebSocketConnect(Session session) {
		System.out.printf("Got connect: %s%n", session);
		this.session = session;
		ConnectMessage cm = new ConnectMessage();
		cm.setClientID("P/example-3559");
		cm.setPassword("aaa");
		cm.setMessageType(MessageType.CONNECT);
		cm.setQos(MessageType.QOS_MOST_ONE);
		ByteBuf out = Unpooled.buffer(50);
		encode(new ChannelHandlerContextTest(), cm, out);
		try {
			Future<Void> fut;
			fut = session.getRemote().sendBytesByFuture(out.nioBuffer());
			fut.get(2, TimeUnit.SECONDS);
		} catch (Throwable t) {
			t.printStackTrace();
		}

		SubscribeMessage submsg = new SubscribeMessage();
		submsg.setMessageID(1);
		submsg.setMessageType(MessageType.SUBSCRIBE);
		submsg.setQos(MessageType.QOS_LEAST_ONE);
		Couple c=new Couple(MessageType.QOS_LEAST_ONE,"G/123");
		submsg.addSubscription(c);
		
		out = Unpooled.buffer(50);
		encode(new ChannelHandlerContextTest(), submsg, out);
		
		try {
			Future<Void> fut;
			fut = session.getRemote().sendBytesByFuture(out.nioBuffer());
			fut.get(2, TimeUnit.SECONDS);
		} catch (Throwable t) {
			t.printStackTrace();
		}
	}

	public void onWebSocketError(Throwable cause) {
		cause.printStackTrace();
	}

	public void onWebSocketText(String message) {
		System.out.printf("Got msg: %s%n", message);
	}
}

class ChannelHandlerContextTest implements ChannelHandlerContext {
	public <T> Attribute<T> attr(AttributeKey<T> key) {
		return null;
	}

	public Channel channel() {
		return null;
	}

	public EventExecutor executor() {
		return null;
	}

	public String name() {
		return null;
	}

	public ChannelHandler handler() {
		return null;
	}

	public boolean isRemoved() {
		return false;
	}

	public ChannelHandlerContext fireChannelRegistered() {
		return null;
	}

	public ChannelHandlerContext fireChannelUnregistered() {
		return null;
	}

	public ChannelHandlerContext fireChannelActive() {
		return null;
	}

	public ChannelHandlerContext fireChannelInactive() {
		return null;
	}

	public ChannelHandlerContext fireExceptionCaught(Throwable cause) {
		return null;
	}

	public ChannelHandlerContext fireUserEventTriggered(Object event) {
		return null;
	}

	public ChannelHandlerContext fireChannelRead(Object msg) {
		return null;
	}

	public ChannelHandlerContext fireChannelReadComplete() {
		return null;
	}

	public ChannelHandlerContext fireChannelWritabilityChanged() {
		return null;
	}

	public ChannelFuture bind(SocketAddress localAddress) {
		return null;
	}

	public ChannelFuture connect(SocketAddress remoteAddress) {
		// TODO Auto-generated method stub
		return null;
	}

	public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress) {
		// TODO Auto-generated method stub
		return null;
	}

	public ChannelFuture disconnect() {
		// TODO Auto-generated method stub
		return null;
	}

	public ChannelFuture close() {
		// TODO Auto-generated method stub
		return null;
	}

	public ChannelFuture deregister() {
		// TODO Auto-generated method stub
		return null;
	}

	public ChannelFuture bind(SocketAddress localAddress, ChannelPromise promise) {
		// TODO Auto-generated method stub
		return null;
	}

	public ChannelFuture connect(SocketAddress remoteAddress, ChannelPromise promise) {
		// TODO Auto-generated method stub
		return null;
	}

	public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise) {
		// TODO Auto-generated method stub
		return null;
	}

	public ChannelFuture disconnect(ChannelPromise promise) {
		// TODO Auto-generated method stub
		return null;
	}

	public ChannelFuture close(ChannelPromise promise) {
		// TODO Auto-generated method stub
		return null;
	}

	public ChannelFuture deregister(ChannelPromise promise) {
		// TODO Auto-generated method stub
		return null;
	}

	public ChannelHandlerContext read() {
		// TODO Auto-generated method stub
		return null;
	}

	public ChannelFuture write(Object msg) {
		// TODO Auto-generated method stub
		return null;
	}

	public ChannelFuture write(Object msg, ChannelPromise promise) {
		// TODO Auto-generated method stub
		return null;
	}

	public ChannelHandlerContext flush() {
		// TODO Auto-generated method stub
		return null;
	}

	public ChannelFuture writeAndFlush(Object msg, ChannelPromise promise) {
		// TODO Auto-generated method stub
		return null;
	}

	public ChannelFuture writeAndFlush(Object msg) {
		// TODO Auto-generated method stub
		return null;
	}

	public ChannelPipeline pipeline() {
		// TODO Auto-generated method stub
		return null;
	}

	public ByteBufAllocator alloc() {
		return UnpooledByteBufAllocator.DEFAULT;
	}

	public ChannelPromise newPromise() {
		// TODO Auto-generated method stub
		return null;
	}

	public ChannelProgressivePromise newProgressivePromise() {
		// TODO Auto-generated method stub
		return null;
	}

	public ChannelFuture newSucceededFuture() {
		// TODO Auto-generated method stub
		return null;
	}

	public ChannelFuture newFailedFuture(Throwable cause) {
		// TODO Auto-generated method stub
		return null;
	}

	public ChannelPromise voidPromise() {
		// TODO Auto-generated method stub
		return null;
	}
}
