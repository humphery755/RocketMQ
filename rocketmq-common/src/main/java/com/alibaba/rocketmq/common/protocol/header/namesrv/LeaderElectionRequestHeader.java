package com.alibaba.rocketmq.common.protocol.header.namesrv;

import com.alibaba.rocketmq.common.protocol.body.LeaderElectionBody;
import com.alibaba.rocketmq.remoting.CommandCustomHeader;
import com.alibaba.rocketmq.remoting.annotation.CFNotNull;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-1
 */
public class LeaderElectionRequestHeader implements CommandCustomHeader {
	public enum ServerState {
        LOOKING, FOLLOWING, LEADING, OBSERVING;
    }
	
	@CFNotNull
	private int code;
	
    @CFNotNull
    private long sid;
    
    private long zxid;
    
    private long electionEpoch;
    //选举轮数
    @CFNotNull
    private long logicalclock;
    
    @CFNotNull
    private ServerState state;

	private LeaderElectionBody body;

    @Override
    public void checkFields() throws RemotingCommandException {
    }


	public long getSid() {
		return sid;
	}


	public void setSid(long id) {
		this.sid = id;
	}


	public long getLogicalclock() {
		return logicalclock;
	}


	public void setLogicalclock(long logicalclock) {
		this.logicalclock = logicalclock;
	}


	public int getCode() {
		return code;
	}


	public void setCode(int code) {
		this.code = code;
	}


	public ServerState getState() {
		return state;
	}


	public void setState(ServerState state) {
		this.state = state;
	}

    
    public LeaderElectionBody getBody() {
		return body;
	}


	public void setBody(LeaderElectionBody body) {
		this.body = body;
	}
}
