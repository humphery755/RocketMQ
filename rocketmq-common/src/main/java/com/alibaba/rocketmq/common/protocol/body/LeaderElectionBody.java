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

import java.util.HashMap;
import java.util.Map;

import com.alibaba.rocketmq.remoting.annotation.CFNotNull;
import com.alibaba.rocketmq.remoting.protocol.RemotingSerializable;


/**
 * @author: manhong.yqd<jodie.yqd@gmail.com>
 * @since: 14-08-06
 */
public class LeaderElectionBody extends RemotingSerializable {
	//被推荐的leader的id
	private long leader;
  //被推荐的leader的事务id
    private long zxid;
  //Epoch/logicalclock
    private long electionEpoch;
    
  //current state of sender 
    @CFNotNull
    private int state;

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
    
}
