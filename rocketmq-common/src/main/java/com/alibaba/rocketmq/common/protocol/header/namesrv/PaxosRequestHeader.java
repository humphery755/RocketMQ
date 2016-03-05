package com.alibaba.rocketmq.common.protocol.header.namesrv;

import com.alibaba.rocketmq.common.protocol.body.PaxosNotificationBody;
import com.alibaba.rocketmq.remoting.CommandCustomHeader;
import com.alibaba.rocketmq.remoting.annotation.CFNotNull;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;

/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-1
 */
public class PaxosRequestHeader implements CommandCustomHeader {
	public final static int LOOKING = 0, FOLLOWING = 1, LEADING = 2, OBSERVING = 3,RESET=-1;
	public final static int PAXOS_PING_REQUEST_CODE = -1, PAXOS_PONG_REQUEST_CODE = -2, PAXOS_GET_LEADER_NODE = -3, PAXOS_GET_MASTER_BROKER = -4;

	@CFNotNull
	private int code;

	// 发送者的id
	@CFNotNull
	private long sid;

	PaxosNotificationBody body;

	public PaxosNotificationBody getBody() {
		return body;
	}

	public void setBody(PaxosNotificationBody body) {
		this.body = body;
	}

	@Override
	public void checkFields() throws RemotingCommandException {
	}

	public int getCode() {
		return code;
	}

	public void setCode(int code) {
		this.code = code;
	}

	public long getSid() {
		return sid;
	}

	public void setSid(long sid) {
		this.sid = sid;
	}
}
