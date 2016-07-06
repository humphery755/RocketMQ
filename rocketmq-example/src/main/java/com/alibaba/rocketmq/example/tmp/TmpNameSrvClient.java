package com.alibaba.rocketmq.example.tmp;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.protocol.RequestCode;
import com.alibaba.rocketmq.common.protocol.header.namesrv.GetKVConfigRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.namesrv.GetRouteInfoRequestHeader;
import com.alibaba.rocketmq.common.protocol.heartbeat.ConsumeType;
import com.alibaba.rocketmq.common.protocol.heartbeat.ConsumerData;
import com.alibaba.rocketmq.common.protocol.heartbeat.HeartbeatData;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import com.alibaba.rocketmq.common.protocol.heartbeat.ProducerData;
import com.alibaba.rocketmq.common.protocol.heartbeat.SubscriptionData;
import com.alibaba.rocketmq.common.protocol.route.BrokerData;
import com.alibaba.rocketmq.common.protocol.route.TopicRouteData;
import com.alibaba.rocketmq.remoting.netty.NettyDecoder;
import com.alibaba.rocketmq.remoting.netty.NettyEncoder;
import com.alibaba.rocketmq.remoting.netty.ResponseFuture;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;

public class TmpNameSrvClient {
	
	private final String host;
	private final int port;
	private final Client client;

	public TmpNameSrvClient(Client c,String host, int port) {
		this.client = c;
		this.host = host;
		this.port = port;
	}

	public void start() throws Exception {
		EventLoopGroup group = new NioEventLoopGroup();
		try {
			Bootstrap b = new Bootstrap();
			b.group(group);
			b.channel(NioSocketChannel.class);
			b.remoteAddress(new InetSocketAddress(host, port));
			b.handler(new ChannelInitializer<SocketChannel>() {

				public void initChannel(SocketChannel ch) throws Exception {
					NameSrvClientHandler handler=new NameSrvClientHandler();
					handler.client=client;
					ch.pipeline().addLast(//
							new NettyEncoder(), //
							new NettyDecoder(), handler); //
					// new IdleStateHandler(0, 0,
					// nettyClientConfig.getClientChannelMaxIdleTimeSeconds()),//
					// new NettyConnetManageHandler(), //
					// new NettyClientHandler());
				}
			});
			ChannelFuture f = b.connect().sync();
			f.addListener(new ChannelFutureListener() {

				public void operationComplete(ChannelFuture future)
						throws Exception {
					if (future.isSuccess()) {
						System.out.println("client connected");
					} else {
						System.out.println("server attemp failed");
						future.cause().printStackTrace();
					}

				}
			});
			f.channel().closeFuture().sync();
		} finally {
			group.shutdownGracefully().sync();
		}
	}

}

@Sharable
class NameSrvClientHandler extends SimpleChannelInboundHandler<RemotingCommand> {
	Map<Integer, ResponseFuture> responseTable = new ConcurrentHashMap();
	Client client;
	/**
	 * 此方法会在连接到server后被调用
	 * */
	public void channelActive(ChannelHandlerContext ctx) {
		GetKVConfigRequestHeader rh = new GetKVConfigRequestHeader();
		rh.setKey("192.168.88.54");
		rh.setNamespace("PROJECT_CONFIG");
		RemotingCommand request = RemotingCommand.createRequestCommand(
				RequestCode.GET_KV_CONFIG, rh);

		final ResponseFuture responseFuture = new ResponseFuture(
				request.getOpaque(), 3000, null, null);

		responseTable.put(request.getOpaque(), responseFuture);
		ctx.writeAndFlush(request);

		// ctx.write(Unpooled.copiedBuffer("Netty rocks!", CharsetUtil.UTF_8));
	}

	/**
	 * 此方法会在接收到server数据后调用
	 * */
	public void channelRead0(ChannelHandlerContext ctx, RemotingCommand cmd) {
		// System.out.println("Client received: " +
		// ByteBufUtil.hexDump(in.readBytes(in.readableBytes())));
		if (cmd != null) {
			switch (cmd.getType()) {
			case REQUEST_COMMAND:
				// processRequestCommand(ctx, cmd);
				System.out.println("Client received: REQUEST_COMMAND " + cmd);
				break;
			case RESPONSE_COMMAND:
				// processResponseCommand(ctx, cmd);
				System.out.println("Client received: RESPONSE_COMMAND " + cmd);
				
				ResponseFuture responseFuture = responseTable.get(cmd
						.getOpaque());
				responseFuture.setResponseCommand(cmd);
				responseFuture.release();
				responseFuture.putResponse(cmd);

				byte[] body = cmd.getBody();
	            
	            
				switch (cmd.getOpaque()) {
				case 0:
					getRoute(ctx,"TopicTest1");
					break;
				case 2:
					buildRouteData(body);
					getRoute(ctx,"%RETRY%please_rename_unique_group_name_3");
					break;
				case 4:
					buildRouteData(body);
					getRoute(ctx,"TBW102");
					break;
				case 6:
					client.start();
					break;
				default:
					break;
				}

				break;
			default:
				System.out.println("Client received: default " + cmd);
				break;
			}
		}
	}
	
	private void buildRouteData(byte[] body){
		if (body != null) {
        	TopicRouteData trd = TopicRouteData.decode(body, TopicRouteData.class);
        	for(BrokerData bd:trd.getBrokerDatas()){
        		for(Map.Entry<Long, String> entry:bd.getBrokerAddrs().entrySet())
        			client.getChannel(entry.getValue());
        	}
        	
        }
		
	}

	private void getRoute(ChannelHandlerContext ctx,String topic) {
		GetRouteInfoRequestHeader requestHeader = new GetRouteInfoRequestHeader();
		requestHeader.setTopic(topic);
		RemotingCommand request = RemotingCommand.createRequestCommand(
				RequestCode.GET_ROUTEINTO_BY_TOPIC, requestHeader);
		final ResponseFuture responseFuture = new ResponseFuture(
				request.getOpaque(), 3000, null, null);

		responseTable.put(request.getOpaque(), responseFuture);
		ctx.writeAndFlush(request);
	}
	

	/**
	 * 捕捉到异常
	 * */
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
		cause.printStackTrace();
		ctx.close();
	}

}