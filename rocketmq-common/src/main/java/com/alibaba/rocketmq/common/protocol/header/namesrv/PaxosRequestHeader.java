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
	
	private long leader;
	
    @CFNotNull
    private long sid;
    
    private long zxid;
    
    private long electionEpoch;
    
    @CFNotNull
    private int state;

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

	public int getCode() {
		return code;
	}

	public void setCode(int code) {
		this.code = code;
	}

	public int getState() {
		return state;
	}

	public void setState(int state) {
		this.state = state;
	}

    public LeaderElectionBody getBody() {
		return body;
	}

	public void setBody(LeaderElectionBody body) {
		this.body = body;
	}

	public long getElectionEpoch() {
		return electionEpoch;
	}

	public void setElectionEpoch(long electionEpoch) {
		this.electionEpoch = electionEpoch;
	}

	public long getLeader() {
		return leader;
	}

	public void setLeader(long leader) {
		this.leader = leader;
	}

	public long getZxid() {
		return zxid;
	}

	public void setZxid(long zxid) {
		this.zxid = zxid;
	}

	@Override
	public String toString() {
		return "PaxosRequestHeader [code=" + code + ", leader=" + leader + ", sid=" + sid + ", zxid=" + zxid + ", electionEpoch=" + electionEpoch
				+ ", state=" + state + "]";
	}
}
