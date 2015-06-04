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
package com.alibaba.rocketmq.namesrv.processor;

import io.netty.channel.ChannelHandlerContext;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.common.ThreadFactoryImpl;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.protocol.RequestCode;
import com.alibaba.rocketmq.common.protocol.ResponseCode;
import com.alibaba.rocketmq.common.protocol.body.LeaderElectionBody;
import com.alibaba.rocketmq.common.protocol.header.namesrv.PaxosRequestHeader;
import com.alibaba.rocketmq.namesrv.PaxosController;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;
import com.alibaba.rocketmq.remoting.netty.NettyRequestProcessor;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;

/**
 * Paxos Server网络请求处理
 * @author humphery
 *
 */
public class PaxosRequestProcessor implements NettyRequestProcessor {
	private static final Logger log = LoggerFactory
			.getLogger(LoggerName.NamesrvLoggerName);
	/*
	 * Maximum capacity of thread queues
	 */
	static final int RECV_CAPACITY = 100;
	public final ArrayBlockingQueue<PaxosRequestHeader> recvQueue = new ArrayBlockingQueue<PaxosRequestHeader>(
			RECV_CAPACITY);
	// 定时线程
    private final ScheduledExecutorService scheduledExecutorService = Executors
        .newSingleThreadScheduledExecutor(new ThreadFactoryImpl("PaxosScheduledThread"));

	private final PaxosController paxosController;

	public PaxosRequestProcessor(PaxosController paxosController) {
		this.paxosController=paxosController;
	}
	
	public void initialize(){
		
	}
	
	public void start() throws Exception {
    }


    public void shutdown() {
        this.scheduledExecutorService.shutdown();
    }

	@Override
	public RemotingCommand processRequest(ChannelHandlerContext ctx,
			RemotingCommand request) throws RemotingCommandException {
		final RemotingCommand response = RemotingCommand.createResponseCommand(null);
		
		final PaxosRequestHeader requestHeader = (PaxosRequestHeader) request
				.decodeCommandCustomHeader(PaxosRequestHeader.class);
		LeaderElectionBody leaderElectionBody;
		if (request.getBody() != null) {
			leaderElectionBody = LeaderElectionBody.decode(request.getBody(),
					LeaderElectionBody.class);
			requestHeader.setBody(leaderElectionBody);
		}

		switch (requestHeader.getCode()) {
		case RequestCode.HEART_BEAT:
			paxosController.registerNsSrv(requestHeader.getSid(), RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
			response.setCode(ResponseCode.SUCCESS);
			break;
		default:
			if(paxosController.getFastLeaderElection().putRequest(requestHeader)){
				// response.setBody(body);
				response.setCode(ResponseCode.SUCCESS);
			}else{
				response.setCode(ResponseCode.SYSTEM_BUSY);
			}
			break;
		}

		response.setRemark(null);
		return response;
	}

}
