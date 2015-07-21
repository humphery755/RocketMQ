/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.remoting.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;

import java.net.InetSocketAddress;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.remoting.ChannelEventListener;
import com.alibaba.rocketmq.remoting.InvokeCallback;
import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.remoting.RemotingServer;
import com.alibaba.rocketmq.remoting.common.Pair;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.alibaba.rocketmq.remoting.common.RemotingUtil;
import com.alibaba.rocketmq.remoting.exception.RemotingSendRequestException;
import com.alibaba.rocketmq.remoting.exception.RemotingTimeoutException;
import com.alibaba.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import com.alibaba.rocketmq.remoting.netty.NettyRemotingServer.NettyConnetManageHandler;
import com.alibaba.rocketmq.remoting.netty.NettyRemotingServer.NettyServerHandler;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;

/**
 * Remoting服务端实现
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-13
 */
public class NettyUDPServer extends NettyRemotingAbstract implements RemotingServer {
	private static final Logger log = LoggerFactory.getLogger(RemotingHelper.RemotingLogName);
	private final Bootstrap serverBootstrap;
	private final EventLoopGroup eventLoopGroupWorker;
	private final NettyServerConfig nettyServerConfig;
	// 处理Callback应答器
	private final ExecutorService publicExecutor;
	private final ChannelEventListener channelEventListener;
	// 定时器
	private final Timer timer = new Timer("ServerHouseKeepingService", true);
	private DefaultEventExecutorGroup defaultEventExecutorGroup;

	private RPCHook rpcHook;

	// 本地server绑定的端口
	private int port = 0;

	public NettyUDPServer(final NettyServerConfig nettyServerConfig) {
		this(nettyServerConfig, null);
	}

	public NettyUDPServer(final NettyServerConfig nettyServerConfig, final ChannelEventListener channelEventListener) {
		super(nettyServerConfig.getServerOnewaySemaphoreValue(), nettyServerConfig.getServerAsyncSemaphoreValue());
		this.serverBootstrap = new Bootstrap();
		this.nettyServerConfig = nettyServerConfig;
		this.channelEventListener = channelEventListener;

		int publicThreadNums = nettyServerConfig.getServerCallbackExecutorThreads();
		if (publicThreadNums <= 0) {
			publicThreadNums = 4;
		}

		this.publicExecutor = Executors.newFixedThreadPool(publicThreadNums, new ThreadFactory() {
			private AtomicInteger threadIndex = new AtomicInteger(0);

			@Override
			public Thread newThread(Runnable r) {
				return new Thread(r, "NettyServerPublicExecutor_" + this.threadIndex.incrementAndGet());
			}
		});

		this.eventLoopGroupWorker = new NioEventLoopGroup(nettyServerConfig.getServerSelectorThreads(), new ThreadFactory() {
			private AtomicInteger threadIndex = new AtomicInteger(0);
			private int threadTotal = nettyServerConfig.getServerSelectorThreads();

			@Override
			public Thread newThread(Runnable r) {
				return new Thread(r, String.format("NettyServerSelector_%d_%d", threadTotal, this.threadIndex.incrementAndGet()));
			}
		});
	}

	@Override
	public void start() {
		this.defaultEventExecutorGroup = new DefaultEventExecutorGroup(//
				nettyServerConfig.getServerWorkerThreads(), //
				new ThreadFactory() {

					private AtomicInteger threadIndex = new AtomicInteger(0);

					@Override
					public Thread newThread(Runnable r) {
						return new Thread(r, "NettyServerWorkerThread_" + this.threadIndex.incrementAndGet());
					}
				});

		Bootstrap childHandler = //
		this.serverBootstrap.group(this.eventLoopGroupWorker).channel(NioDatagramChannel.class)
		//
				.option(ChannelOption.SO_BACKLOG, 1024)
				//
				.option(ChannelOption.SO_REUSEADDR, true)
				//
				.option(ChannelOption.SO_KEEPALIVE, false)
				//
				.option(ChannelOption.SO_BROADCAST, true)
				//
				.option(ChannelOption.SO_SNDBUF, nettyServerConfig.getServerSocketSndBufSize())
				//
				.option(ChannelOption.SO_RCVBUF, nettyServerConfig.getServerSocketRcvBufSize())
				//
				.localAddress(new InetSocketAddress(this.nettyServerConfig.getListenUDPPort()))
				.handler(new ChannelInitializer<DatagramChannel>() {
					@Override
					public void initChannel(DatagramChannel ch) throws Exception {
						ch.pipeline().addLast(
						//
								defaultEventExecutorGroup, //
								new UDP2BufAdapt(),
								new NettyEncoder(), //
								new NettyDecoder(), //
								//new IdleStateHandler(0, 0, nettyServerConfig.getServerChannelMaxIdleTimeSeconds()),//
								//new NettyConnetManageHandler(), //
								new NettyServerHandler());
					}
				});

		if (nettyServerConfig.isServerPooledByteBufAllocatorEnable()) {
			// 这个选项有可能会占用大量堆外内存，暂时不使用。
			childHandler.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
		}

		try {
			ChannelFuture sync = this.serverBootstrap.bind().sync();
			InetSocketAddress addr = (InetSocketAddress) sync.channel().localAddress();
			this.port = addr.getPort();
		} catch (InterruptedException e1) {
			throw new RuntimeException("this.serverBootstrap.bind().sync() InterruptedException", e1);
		}

		if (this.channelEventListener != null) {
			this.nettyEventExecuter.start();
		}

		// 每隔1秒扫描下异步调用超时情况
		this.timer.scheduleAtFixedRate(new TimerTask() {

			@Override
			public void run() {
				try {
					NettyUDPServer.this.scanResponseTable();
				} catch (Exception e) {
					log.error("scanResponseTable exception", e);
				}
			}
		}, 1000 * 3, 1000);
	}

	@Override
	public void registerProcessor(int requestCode, NettyRequestProcessor processor, ExecutorService executor) {
		ExecutorService executorThis = executor;
		if (null == executor) {
			executorThis = this.publicExecutor;
		}

		Pair<NettyRequestProcessor, ExecutorService> pair = new Pair<NettyRequestProcessor, ExecutorService>(processor, executorThis);
		this.processorTable.put(requestCode, pair);
	}

	@Override
	public void registerDefaultProcessor(NettyRequestProcessor processor, ExecutorService executor) {
		this.defaultRequestProcessor = new Pair<NettyRequestProcessor, ExecutorService>(processor, executor);
	}

	@Override
	public RemotingCommand invokeSync(final Channel channel, final RemotingCommand request, final long timeoutMillis) throws InterruptedException,
			RemotingSendRequestException, RemotingTimeoutException {
		return this.invokeSyncImpl(channel, request, timeoutMillis);
	}

	@Override
	public void invokeAsync(Channel channel, RemotingCommand request, long timeoutMillis, InvokeCallback invokeCallback) throws InterruptedException,
			RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
		this.invokeAsyncImpl(channel, request, timeoutMillis, invokeCallback);
	}

	@Override
	public void invokeOneway(Channel channel, RemotingCommand request, long timeoutMillis) throws InterruptedException,
			RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
		this.invokeOnewayImpl(channel, request, timeoutMillis);
	}

	@Override
	public void shutdown() {
		try {
			if (this.timer != null) {
				this.timer.cancel();
			}

			this.eventLoopGroupWorker.shutdownGracefully();

			if (this.nettyEventExecuter != null) {
				this.nettyEventExecuter.shutdown();
			}

			if (this.defaultEventExecutorGroup != null) {
				this.defaultEventExecutorGroup.shutdownGracefully();
			}
		} catch (Exception e) {
			log.error("NettyRemotingServer shutdown exception, ", e);
		}

		if (this.publicExecutor != null) {
			try {
				this.publicExecutor.shutdown();
			} catch (Exception e) {
				log.error("NettyRemotingServer shutdown exception, ", e);
			}
		}
	}

	@Override
	public ChannelEventListener getChannelEventListener() {
		return channelEventListener;
	}

	@Override
	public ExecutorService getCallbackExecutor() {
		return this.publicExecutor;
	}

	class UDP2BufAdapt extends SimpleChannelInboundHandler<DatagramPacket> {

		   /* @Override
		    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
		        cause.printStackTrace();
		        // We don't close the channel because we can keep serving requests.
		    }*/
			@Override
			protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket msg) throws Exception {
				if (msg instanceof DatagramPacket) {
					ByteBuf data=null;
		            try {
		            	DatagramPacket dp = (DatagramPacket) msg;
		            	data = dp.content().copy();
		            } catch (DecoderException e) {
		                throw e;
		            } catch (Throwable t) {
		                throw new DecoderException(t);
		            } finally {
		                ctx.fireChannelRead(data);
		            }
		        } else {
		            ctx.fireChannelRead(msg);
		        }
				System.out.println("hello world!");
			}
		}
	
	class NettyServerHandler extends SimpleChannelInboundHandler<RemotingCommand> {

		@Override
		protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
			processMessageReceived(ctx, msg);
		}
	}

	class NettyConnetManageHandler extends ChannelDuplexHandler {
		@Override
		public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
			final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
			log.info("NETTY SERVER PIPELINE: channelRegistered {}", remoteAddress);
			super.channelRegistered(ctx);
		}

		@Override
		public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
			final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
			log.info("NETTY SERVER PIPELINE: channelUnregistered, the channel[{}]", remoteAddress);
			super.channelUnregistered(ctx);
		}

		@Override
		public void channelActive(ChannelHandlerContext ctx) throws Exception {
			final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
			log.info("NETTY SERVER PIPELINE: channelActive, the channel[{}]", remoteAddress);
			super.channelActive(ctx);

			if (NettyUDPServer.this.channelEventListener != null) {
				NettyUDPServer.this.putNettyEvent(new NettyEvent(NettyEventType.CONNECT, remoteAddress.toString(), ctx.channel()));
			}
		}

		@Override
		public void channelInactive(ChannelHandlerContext ctx) throws Exception {
			final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
			log.info("NETTY SERVER PIPELINE: channelInactive, the channel[{}]", remoteAddress);
			super.channelInactive(ctx);

			if (NettyUDPServer.this.channelEventListener != null) {
				NettyUDPServer.this.putNettyEvent(new NettyEvent(NettyEventType.CLOSE, remoteAddress.toString(), ctx.channel()));
			}
		}

		@Override
		public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
			if (evt instanceof IdleStateEvent) {
				IdleStateEvent evnet = (IdleStateEvent) evt;
				if (evnet.state().equals(IdleState.ALL_IDLE)) {
					final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
					log.warn("NETTY SERVER PIPELINE: IDLE exception [{}]", remoteAddress);
					RemotingUtil.closeChannel(ctx.channel());
					if (NettyUDPServer.this.channelEventListener != null) {
						NettyUDPServer.this.putNettyEvent(new NettyEvent(NettyEventType.IDLE, remoteAddress.toString(), ctx.channel()));
					}
				}
			}

			ctx.fireUserEventTriggered(evt);
		}

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
			final String remoteAddress = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
			log.warn("NETTY SERVER PIPELINE: exceptionCaught {}", remoteAddress);
			log.warn("NETTY SERVER PIPELINE: exceptionCaught exception.", cause);

			if (NettyUDPServer.this.channelEventListener != null) {
				NettyUDPServer.this.putNettyEvent(new NettyEvent(NettyEventType.EXCEPTION, remoteAddress.toString(), ctx.channel()));
			}

			RemotingUtil.closeChannel(ctx.channel());
		}
	}

	@Override
	public void registerRPCHook(RPCHook rpcHook) {
		this.rpcHook = rpcHook;
	}

	@Override
	public RPCHook getRPCHook() {
		return this.rpcHook;
	}

	@Override
	public int localListenPort() {
		return this.port;
	}

	@Override
	public Pair<NettyRequestProcessor, ExecutorService> getProcessorPair(int requestCode) {
		return processorTable.get(requestCode);
	}
}
