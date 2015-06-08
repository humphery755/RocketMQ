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
package com.alibaba.rocketmq.namesrv;

import io.netty.channel.Channel;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.namesrv.NamesrvConfig;
import com.alibaba.rocketmq.common.protocol.RequestCode;
import com.alibaba.rocketmq.common.protocol.ResponseCode;
import com.alibaba.rocketmq.common.protocol.header.namesrv.PaxosRequestHeader;
import com.alibaba.rocketmq.namesrv.paxos.FastLeaderElection;
import com.alibaba.rocketmq.namesrv.processor.PaxosRequestProcessor;
import com.alibaba.rocketmq.remoting.ChannelEventListener;
import com.alibaba.rocketmq.remoting.RemotingClient;
import com.alibaba.rocketmq.remoting.RemotingServer;
import com.alibaba.rocketmq.remoting.exception.RemotingConnectException;
import com.alibaba.rocketmq.remoting.exception.RemotingSendRequestException;
import com.alibaba.rocketmq.remoting.exception.RemotingTimeoutException;
import com.alibaba.rocketmq.remoting.netty.NettyClientConfig;
import com.alibaba.rocketmq.remoting.netty.NettyRemotingClient;
import com.alibaba.rocketmq.remoting.netty.NettyServerConfig;
import com.alibaba.rocketmq.remoting.netty.NettyUDPServer;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;

/**
 * Name Server服务控制
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-5
 */
public class PaxosController {
	private static final Logger log = LoggerFactory
			.getLogger(LoggerName.NamesrvLoggerName);

	private final NamesrvController namesrvController;
	private FastLeaderElection fastLeaderElection;
	private String[] allNsAddrs;
	private long myid;
	// 服务端通信层对象
    private RemotingServer remotingServer;
	
	private Map<Long, String> nsServers = new HashMap<Long, String>();
	
	private final PaxosRequestProcessor paxosRequestProcessor;
	private RemotingClient remotingClient;

	public PaxosController(NamesrvController namesrvController) {
		this.namesrvController = namesrvController;
		myid = namesrvController.getNamesrvConfig().getMyid();
		if(namesrvController.getNamesrvConfig().getNamesrvAddr()==null){
			log.error("namesrvAddr can't null!");
			System.exit(1);
		}
		allNsAddrs = namesrvController.getNamesrvConfig().getNamesrvAddr().split(";");
		paxosRequestProcessor = new PaxosRequestProcessor(this);
		fastLeaderElection = new FastLeaderElection(this);
		
		NettyClientConfig nettyClientConfig = new NettyClientConfig();
		remotingClient = new NettyRemotingClient(nettyClientConfig);
		// remotingClient.registerRPCHook(rpcHook);
	}

	public boolean initialize() {

		// 初始化通信层
        this.remotingServer = new NettyUDPServer(namesrvController.getNettyServerConfig(), new ChannelEventListener(){

			@Override
			public void onChannelConnect(String remoteAddr, Channel channel) {
			}

			@Override
			public void onChannelClose(String remoteAddr, Channel channel) {
			}

			@Override
			public void onChannelException(String remoteAddr, Channel channel) {
			}

			@Override
			public void onChannelIdle(String remoteAddr, Channel channel) {
			}
        	
        });
        
		this.registerProcessor();

		this.namesrvController.getScheduledExecutorService()
				.scheduleAtFixedRate(new Runnable() {
					@Override
					public void run() {
						try {
							//PaxosController.this.connectAll();
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}, 1, 30, TimeUnit.SECONDS);

		return true;
	}

	private void registerProcessor() {
		remotingServer.registerProcessor(
				RequestCode.PAXOS_ALGORITHM_REQUEST_CODE,
				paxosRequestProcessor,
				namesrvController.getScheduledExecutorService());
	}

	public void start() throws Exception {
		this.remotingServer.start();
		remotingClient.start();
		fastLeaderElection.start();
		paxosRequestProcessor.start();
	}

	public void shutdown() {
		this.remotingServer.shutdown();
		paxosRequestProcessor.shutdown();
		fastLeaderElection.shutdown();
	}
	
	public void registerNsSrv(Long id,String addr){
		nsServers.put(id, addr);
	}

	private void connectAll() throws RemotingConnectException,
			RemotingSendRequestException, RemotingTimeoutException,
			InterruptedException {
		PaxosRequestHeader rHeader = new PaxosRequestHeader();
		rHeader.setSid(myid);
		rHeader.setCode(RequestCode.HEART_BEAT);
		RemotingCommand request = RemotingCommand.createRequestCommand(
				RequestCode.PAXOS_ALGORITHM_REQUEST_CODE, rHeader);

		for (String addr : allNsAddrs) {
			RemotingCommand response = remotingClient.invokeSync(addr, request,
					3000);
			assert response != null;
			switch (response.getCode()) {
			case ResponseCode.SUCCESS: {
				return;
			}
			default:
				break;
			}
		}
	}

	public NamesrvConfig getNamesrvConfig() {
		return namesrvController.getNamesrvConfig();
	}

	public NettyServerConfig getNettyServerConfig() {
		return namesrvController.getNettyServerConfig();
	}

	public RemotingServer getRemotingServer() {
		return namesrvController.getRemotingServer();
	}

	public FastLeaderElection getFastLeaderElection() {
		return fastLeaderElection;
	}

	public Map<Long, String> getNsServers() {
		return nsServers;
	}

	public RemotingClient getRemotingClient() {
		return remotingClient;
	}

	public long getMyid() {
		return myid;
	}

	public String[] getAllNsAddrs() {
		return allNsAddrs;
	}
	
}
