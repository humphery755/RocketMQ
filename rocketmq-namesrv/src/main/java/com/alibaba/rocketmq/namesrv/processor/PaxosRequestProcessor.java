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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.protocol.RequestCode;
import com.alibaba.rocketmq.common.protocol.ResponseCode;
import com.alibaba.rocketmq.common.protocol.body.PaxosNotificationBody;
import com.alibaba.rocketmq.common.protocol.header.namesrv.PaxosRequestHeader;
import com.alibaba.rocketmq.namesrv.PaxosController;
import com.alibaba.rocketmq.namesrv.paxos.Server;
import com.alibaba.rocketmq.namesrv.paxos.Server.State;
import com.alibaba.rocketmq.remoting.common.RemotingUtil;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;
import com.alibaba.rocketmq.remoting.exception.RemotingConnectException;
import com.alibaba.rocketmq.remoting.exception.RemotingSendRequestException;
import com.alibaba.rocketmq.remoting.exception.RemotingTimeoutException;
import com.alibaba.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import com.alibaba.rocketmq.remoting.netty.NettyRequestProcessor;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;

import io.netty.channel.ChannelHandlerContext;

/**
 * Paxos Server网络请求处理
 * 
 * @author humphery
 *
 */
public class PaxosRequestProcessor implements NettyRequestProcessor {
	private static final Logger log = LoggerFactory.getLogger(LoggerName.NamesrvLoggerName);

	private final PaxosController paxosController;

	public PaxosRequestProcessor(PaxosController paxosController) {
		this.paxosController = paxosController;
	}

	public void initialize() {

	}

	public void start() throws Exception {
	}

	public void shutdown() {
	}

	@Override
	public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
		final RemotingCommand response = RemotingCommand.createResponseCommand(null);

		final PaxosRequestHeader requestHeader = (PaxosRequestHeader) request.decodeCommandCustomHeader(PaxosRequestHeader.class);
		PaxosNotificationBody paxosBody = null;
		if (request.getBody() != null) {
			paxosBody = PaxosNotificationBody.decode(request.getBody(), PaxosNotificationBody.class);
			requestHeader.setBody(paxosBody);
		}

		switch (requestHeader.getCode()) {
		case PaxosRequestHeader.PAXOS_GET_LEADER_NODE:
			paxosBody = new PaxosNotificationBody();
			paxosBody.setLeader(paxosController.getFastLeaderElection().getLeader());
			response.setBody(paxosBody.encode());
			response.setCode(ResponseCode.SUCCESS);
			return response;
		case PaxosRequestHeader.PAXOS_PING_REQUEST_CODE: { // ping
			Server s = paxosController.getNsServers().get(requestHeader.getSid());
			PaxosRequestHeader req = new PaxosRequestHeader();
			req.setSid(paxosController.getMyid());
			req.setCode(PaxosRequestHeader.PAXOS_PONG_REQUEST_CODE);

			RemotingCommand reqcmd = RemotingCommand.createRequestCommand(RequestCode.PAXOS_ALGORITHM_REQUEST_CODE, req);
			PaxosNotificationBody body = new PaxosNotificationBody();
			body.setAddr(RemotingUtil.getLocalAddress() + ":" + paxosController.getNettyServerConfig().getListenPort());
			reqcmd.setBody(body.encode());
			try {
				paxosController.getRemotingClient().invokeOneway(s.getAddr(), reqcmd, 3000);
			} catch (RemotingConnectException | RemotingTooMuchRequestException | RemotingTimeoutException | RemotingSendRequestException | InterruptedException e) {
				log.error("",e);
				response.setCode(ResponseCode.SYSTEM_ERROR);
				return response;
			}
			response.setCode(ResponseCode.SUCCESS);
			return response;
		}
		case PaxosRequestHeader.PAXOS_PONG_REQUEST_CODE: // pong
			// String addr=paxosBody.getAddr();
			Server s = paxosController.getNsServers().get(requestHeader.getSid());
			s.setState(State.ACTIVE);
			response.setCode(ResponseCode.SUCCESS);
			return response;

		default:
			break;
		}

		if (paxosController.getFastLeaderElection().putRequest(requestHeader)) {
			response.setCode(ResponseCode.SUCCESS);
		} else {
			response.setCode(ResponseCode.SYSTEM_BUSY);
		}

		response.setRemark(null);
		return response;
	}

}
