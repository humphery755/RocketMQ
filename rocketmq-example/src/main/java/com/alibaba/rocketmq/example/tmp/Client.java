package com.alibaba.rocketmq.example.tmp;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.alibaba.rocketmq.remoting.netty.NettyDecoder;
import com.alibaba.rocketmq.remoting.netty.NettyEncoder;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

public abstract class Client {
	private static Map<String, Channel> channels = new ConcurrentHashMap();
	abstract void start();
	abstract SimpleChannelInboundHandler<RemotingCommand> getHandler();
	Channel getChannel(String addr){
		Channel c = channels.get(addr);
		if (c != null)
			return c;
		try {
			EventLoopGroup group = new NioEventLoopGroup();
			Bootstrap b = new Bootstrap();
			b.group(group);
			b.channel(NioSocketChannel.class);
			String[] host = addr.split(":");
			String ip = host[0];
			int port = Integer.valueOf(host[1]);
			b.remoteAddress(new InetSocketAddress(ip, port));
			b.handler(new ChannelInitializer<SocketChannel>() {

				public void initChannel(SocketChannel ch) throws Exception {
					// ch.pipeline().addLast();
					ch.pipeline().addLast(//
							new NettyEncoder(), //
							new NettyDecoder(), getHandler()); //
					// new IdleStateHandler(0, 0,
					// nettyClientConfig.getClientChannelMaxIdleTimeSeconds()),//
					// new NettyConnetManageHandler(), //
					// new NettyClientHandler());
				}
			});

			ChannelFuture f = b.connect().sync();

			f.addListener(new ChannelFutureListener() {

				public void operationComplete(ChannelFuture future) throws Exception {
					if (future.isSuccess()) {
						System.out.println("client connected");
					} else {
						System.out.println("server attemp failed");
						future.cause().printStackTrace();
					}

				}
			});
			c = f.channel();
			channels.put(addr, c);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return c;
	}
}
