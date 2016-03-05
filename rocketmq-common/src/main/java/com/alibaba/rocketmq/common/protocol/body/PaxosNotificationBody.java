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
package com.alibaba.rocketmq.common.protocol.body;

import com.alibaba.rocketmq.remoting.annotation.CFNotNull;
import com.alibaba.rocketmq.remoting.protocol.RemotingSerializable;

/**
 * @author: manhong.yqd<jodie.yqd@gmail.com>
 * @since: 14-08-06
 */
public class PaxosNotificationBody extends RemotingSerializable {
	// 被推荐的leader的id
	private long leader;
	// 被推荐的leader的事务id
	private long txid;
	// Epoch/logicalclock
	private long electionEpoch;
	private long peerEpoch;
	@CFNotNull
	private int state;
	private String addr;

	public String getAddr() {
		return addr;
	}

	public void setAddr(String addr) {
		this.addr = addr;
	}

	// current state of sender

	public long getLeader() {
		return leader;
	}

	public void setLeader(long leader) {
		this.leader = leader;
	}

	public long getTxid() {
		return txid;
	}

	public void setTxid(long txid) {
		this.txid = txid;
	}

	public long getElectionEpoch() {
		return electionEpoch;
	}

	public void setElectionEpoch(long electionEpoch) {
		this.electionEpoch = electionEpoch;
	}

	public int getState() {
		return state;
	}

	public void setState(int state) {
		this.state = state;
	}

	public long getPeerEpoch() {
		return peerEpoch;
	}

	public void setPeerEpoch(long peerEpoch) {
		this.peerEpoch = peerEpoch;
	}

}
