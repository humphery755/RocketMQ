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

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.namesrv.NamesrvConfig;
import com.alibaba.rocketmq.common.protocol.RequestCode;
import com.alibaba.rocketmq.namesrv.paxos.FastLeaderElection;
import com.alibaba.rocketmq.namesrv.paxos.Server;
import com.alibaba.rocketmq.namesrv.processor.PaxosRequestProcessor;
import com.alibaba.rocketmq.remoting.RemotingClient;
import com.alibaba.rocketmq.remoting.RemotingServer;
import com.alibaba.rocketmq.remoting.netty.NettyClientConfig;
import com.alibaba.rocketmq.remoting.netty.NettyRemotingClient;
import com.alibaba.rocketmq.remoting.netty.NettyServerConfig;

/**
 * Name Server服务控制
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-5
 */
public class PaxosController {
	private static final Logger log = LoggerFactory.getLogger(LoggerName.NamesrvLoggerName);

	private final NamesrvController namesrvController;
	private FastLeaderElection fastLeaderElection;
	private long myid;
	// 服务端通信层对象
	private RemotingServer remotingServer;
	private Map<Long, Server> nsServers = new HashMap<Long, Server>();

	private final PaxosRequestProcessor paxosRequestProcessor;
	private RemotingClient remotingClient;

	public PaxosController(NamesrvController namesrvController) {
		this.namesrvController = namesrvController;
		myid = namesrvController.getNamesrvConfig().getMyid();
		if (namesrvController.getNamesrvConfig().getNamesrvAddr() == null) {
			log.error("namesrvAddr can't null!");
			System.exit(1);
		}
		String[] allNsAddrs = namesrvController.getNamesrvConfig().getNamesrvAddr().split(";");
		long i = 0;
		for (String addr : allNsAddrs) {
			Server s = new Server(i++, addr);
			nsServers.put(s.getMyid(), s);
		}
		paxosRequestProcessor = new PaxosRequestProcessor(this);
		fastLeaderElection = new FastLeaderElection(this);

		NettyClientConfig nettyClientConfig = new NettyClientConfig();
		remotingClient = new NettyRemotingClient(nettyClientConfig);// new
																	// NettyUDPClient(nettyClientConfig);
		// remotingClient.registerRPCHook(rpcHook);

	}

	public boolean initialize() {
		this.remotingServer = namesrvController.getRemotingServer();
		// 初始化通信层

		/*
		 * new NettyUDPServer(namesrvController.getNettyServerConfig(), new
		 * ChannelEventListener(){ public void onChannelConnect(String
		 * remoteAddr, Channel channel) {} public void onChannelClose(String
		 * remoteAddr, Channel channel) {} public void onChannelException(String
		 * remoteAddr, Channel channel) {} public void onChannelIdle(String
		 * remoteAddr, Channel channel) { } });
		 */

		remotingServer.registerProcessor(RequestCode.PAXOS_ALGORITHM_REQUEST_CODE, paxosRequestProcessor, namesrvController.getScheduledExecutorService());
		namesrvController.getRemotingServer().registerDefaultProcessor(paxosRequestProcessor, namesrvController.getRemotingExecutor());
		return true;
	}

	public void start() throws Exception {
		// this.remotingServer.start();
		remotingClient.start();
		fastLeaderElection.start();
		paxosRequestProcessor.start();
	}

	public void shutdown() {
		// this.remotingServer.shutdown();
		paxosRequestProcessor.shutdown();
		fastLeaderElection.shutdown();
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

	public RemotingClient getRemotingClient() {
		return remotingClient;
	}

	public long getMyid() {
		return myid;
	}

	public boolean isLeader() {
		return fastLeaderElection.getLeader() == myid;
	}

	public Map<Long, Server> getNsServers() {
		return nsServers;
	}
}
