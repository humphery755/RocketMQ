package com.alibaba.rocketmq.namesrv.processor;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
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
import io.netty.util.concurrent.DefaultEventExecutorGroup;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import com.alibaba.rocketmq.common.protocol.RequestCode;
import com.alibaba.rocketmq.common.protocol.header.namesrv.PaxosRequestHeader;
import com.alibaba.rocketmq.remoting.netty.NettyDecoder;
import com.alibaba.rocketmq.remoting.netty.NettyEncoder;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;

public class UDPTest {
	static InetAddress ip=null;
	final static int PORT = 9999;
	private Bootstrap serverBootstrap = new Bootstrap();
	private EventLoopGroup eventLoopGroupWorker = new NioEventLoopGroup(1, new ThreadFactory() {
		private AtomicInteger threadIndex = new AtomicInteger(0);
		private int threadTotal = 1;

		@Override
		public Thread newThread(Runnable r) {
			return new Thread(r, String.format("NettyServerSelector_%d_%d", threadTotal, this.threadIndex.incrementAndGet()));
		}
	});

	private DefaultEventExecutorGroup defaultEventExecutorGroup = new DefaultEventExecutorGroup(1, //
			new ThreadFactory() {

				private AtomicInteger threadIndex = new AtomicInteger(0);

				@Override
				public Thread newThread(Runnable r) {
					return new Thread(r, "NettyServerWorkerThread_" + this.threadIndex.incrementAndGet());
				}
			});

	public UDPTest() throws InterruptedException {
		Bootstrap childHandler = //
		this.serverBootstrap.group(this.eventLoopGroupWorker).channel(NioDatagramChannel.class).option(ChannelOption.SO_BROADCAST, true)
		//
				.localAddress(new InetSocketAddress(9999)).handler(new ChannelInitializer<DatagramChannel>() {
					@Override
					public void initChannel(DatagramChannel ch) throws Exception {
						ch.pipeline().addLast(
						//
								defaultEventExecutorGroup, //
								new RemotingCommandAdapt(),
								new NettyEncoder(), //
								new NettyDecoder(),//
								// new IdleStateHandler(0, 0,
								// nettyServerConfig.getServerChannelMaxIdleTimeSeconds()),//
								// new NettyConnetManageHandler(), //
								new NettyServerHandler()
								);
					}
				});

		try {
			ChannelFuture sync = this.serverBootstrap.bind().sync();
			InetSocketAddress addr = (InetSocketAddress) sync.channel().localAddress();
		} catch (InterruptedException e1) {
			throw new RuntimeException("this.serverBootstrap.bind().sync() InterruptedException", e1);
		}
		
		
/*		EventLoopGroup group = new NioEventLoopGroup();
        try {
            Bootstrap b = new Bootstrap();
            b.group(group)
             .channel(NioDatagramChannel.class)
             .option(ChannelOption.SO_BROADCAST, true)
             .handler(new NettyServerHandler());

            b.bind(PORT).sync().channel().closeFuture();
        } finally {
            //group.shutdownGracefully();
        }*/
	}

	final static NettyEncoder NETTY_ENCODER = new NettyEncoder();
	final static NettyDecoder NETTY_DECODER = new NettyDecoder();
	private void test() throws UnknownHostException, InterruptedException{
		PaxosRequestHeader req = new PaxosRequestHeader();
		req.setElectionEpoch(1);
		req.setLeader(1);
		req.setZxid(1);
		req.setState(1);
		req.setSid(1);

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PAXOS_ALGORITHM_REQUEST_CODE, req);

		
		//ByteBuffer bb = request.encode();
		// 由于数据报的数据是以字符数组传的形式存储的，所以传转数据
		//byte[] buf = bb.array();

		// 确定发送方的IP地址及端口号，地址为本地机器地址
		
		

		/*
		 * // 创建发送类型的数据报： DatagramPacket sendPacket = new DatagramPacket(buf,
		 * buf.length, ip, port);
		 * 
		 * // 通过套接字发送数据： sendSocket.send(sendPacket);
		 * 
		 * sendSocket.close();
		 */

		EventLoopGroup group = new NioEventLoopGroup();
		try {
			Bootstrap b = new Bootstrap();
			b.group(group).channel(NioDatagramChannel.class).option(ChannelOption.SO_BROADCAST, true);
			b.handler(new ChannelInitializer<DatagramChannel>() {
				@Override
				public void initChannel(DatagramChannel ch) throws Exception {
					ch.pipeline().addLast(
					//
							new DefaultEventExecutorGroup(1, //
									new ThreadFactory() {
										private AtomicInteger threadIndex = new AtomicInteger(0);
										@Override
										public Thread newThread(Runnable r) {
											return new Thread(r, "NettyClientWorkerThread_" + this.threadIndex.incrementAndGet());
										}
									}), //
							new UDPClientAdapt(),
							// new IdleStateHandler(0, 0,
							// nettyServerConfig.getServerChannelMaxIdleTimeSeconds()),//
							// new NettyConnetManageHandler(), //
							new NettyServerHandler()
							);
				}
			});
			
			for(int i=0;i<100;i++){

				Channel ch = b.bind(0).sync().channel();
/*				ByteBuf out = Unpooled.buffer();
				try {
					NETTY_ENCODER.encode(null, request, out);
				} catch (Exception e) {
					e.printStackTrace();
				}
				DatagramPacket sendPacket = new DatagramPacket(out,new InetSocketAddress(ip, PORT));*/
			// Broadcast the QOTM request to port 8080.Unpooled.copiedBuffer("QOTM?", CharsetUtil.UTF_8)
				ch.writeAndFlush(request).sync();
				System.out.println("ch.close()!");
				ch.close();
			}

		} finally {
			group.shutdownGracefully();
		}
	}
	
	
	 public static void send(InetAddress ip,int port) throws IOException {
		    // TODO Auto-generated method stub
		     // 创建发送方的套接字，IP默认为本地，端口号随机  
		        java.net.DatagramSocket sendSocket = new java.net.DatagramSocket();
		 
		        // 确定要发送的消息：  
		        String mes = "我是设备！";  
		 
		        // 由于数据报的数据是以字符数组传的形式存储的，所以传转数据  
		        byte[] buf = mes.getBytes("UTF-8");  
		 
		        // 确定发送方的IP地址及端口号，地址为本地机器地址  
		 
		        // 创建发送类型的数据报：  
		        java.net.DatagramPacket sendPacket = new java.net.DatagramPacket(buf, buf.length, ip,  
		                port);  
		 
		        // 通过套接字发送数据：  
		        sendSocket.send(sendPacket);  
		 
		        sendSocket.close();
		  }
	/**
	 * @param args
	 * @throws IOException
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException {
		UDPTest self = new UDPTest();
		ip = InetAddress.getLocalHost();
		// TODO Auto-generated method stub
		// 创建发送方的套接字，IP默认为本地，端口号随机
		self.test();
		
	}
	
	class UDPClientAdapt extends SimpleChannelInboundHandler<RemotingCommand> {

		   /* @Override
		    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
		        cause.printStackTrace();
		        // We don't close the channel because we can keep serving requests.
		    }*/
			@Override
			protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
				if (msg instanceof RemotingCommand) {
					DatagramPacket sendPacket = null;
					RemotingCommand request = (RemotingCommand)msg;
					ByteBuf out = Unpooled.buffer();
					try {
						NETTY_ENCODER.encode(null, request, out);
						sendPacket = new DatagramPacket(out,new InetSocketAddress(ip, PORT));
					} catch (DecoderException e) {
		                throw e;
		            } catch (Throwable t) {
		                throw new DecoderException(t);
		            } finally {
		                ctx.fireChannelRead(sendPacket);
		            }
		        } else {
		            ctx.fireChannelRead(msg);
		        }
				System.out.println("hello world!");
			}
		}

	class RemotingCommandAdapt extends SimpleChannelInboundHandler<DatagramPacket> {

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
			System.out.println("hello world!"+msg);
		}

	   /* @Override
	    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
	        cause.printStackTrace();
	        supe
	        // We don't close the channel because we can keep serving requests.
	    }*/
	}
}
