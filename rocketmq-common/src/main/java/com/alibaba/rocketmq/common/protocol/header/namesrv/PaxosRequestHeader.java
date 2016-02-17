package com.alibaba.rocketmq.common.protocol.header.namesrv;

import com.alibaba.rocketmq.common.protocol.body.LeaderElectionBody;
import com.alibaba.rocketmq.remoting.CommandCustomHeader;
import com.alibaba.rocketmq.remoting.annotation.CFNotNull;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-1
 */
public class PaxosRequestHeader implements CommandCustomHeader {
    public final static int LOOKING=0, FOLLOWING=1, LEADING=2, OBSERVING=3;
	
	@CFNotNull
	private int code;
	
	//发送者的id 
    @CFNotNull
    private long sid;
	
	LeaderElectionBody body;

    public LeaderElectionBody getBody() {
		return body;
	}

	public void setBody(LeaderElectionBody body) {
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
