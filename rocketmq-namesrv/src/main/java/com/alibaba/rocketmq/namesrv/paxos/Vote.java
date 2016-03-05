/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.rocketmq.namesrv.paxos;

import com.alibaba.rocketmq.common.protocol.header.namesrv.PaxosRequestHeader;

public class Vote {

	public Vote(long id, long txid) {
		this.version = 0x0;
		this.id = id;
		this.txid = txid;
		this.electionEpoch = -1;
		this.peerEpoch = -1;
		this.state = PaxosRequestHeader.LOOKING;
	}

	public Vote(long id, long txid, long peerEpoch) {
		this.version = 0x0;
		this.id = id;
		this.txid = txid;
		this.electionEpoch = -1;
		this.peerEpoch = peerEpoch;
		this.state = PaxosRequestHeader.LOOKING;
	}

	public Vote(long id, long txid, long electionEpoch, long peerEpoch) {
		this.version = 0x0;
		this.id = id;
		this.txid = txid;
		this.electionEpoch = electionEpoch;
		this.peerEpoch = peerEpoch;
		this.state = PaxosRequestHeader.LOOKING;
	}

	public Vote(int version, long id, long txid, long electionEpoch, long peerEpoch, int state) {
		this.version = version;
		this.id = id;
		this.txid = txid;
		this.electionEpoch = electionEpoch;
		this.state = state;
		this.peerEpoch = peerEpoch;
	}

	public Vote(long id, long txid, long electionEpoch, long peerEpoch, int state) {
		this.id = id;
		this.txid = txid;
		this.electionEpoch = electionEpoch;
		this.state = state;
		this.peerEpoch = peerEpoch;
		this.version = 0x0;
	}

	final private int version;

	final private long id;

	final private long txid;

	final private long electionEpoch;

	final private long peerEpoch;

	public int getVersion() {
		return version;
	}

	public long getId() {
		return id;
	}

	public long getTxid() {
		return txid;
	}

	public long getElectionEpoch() {
		return electionEpoch;
	}

	public long getPeerEpoch() {
		return peerEpoch;
	}

	public int getState() {
		return state;
	}

	final private int state;

	@Override
	public boolean equals(Object o) {
		if (!(o instanceof Vote)) {
			return false;
		}
		Vote other = (Vote) o;
		return (id == other.id && txid == other.txid && electionEpoch == other.electionEpoch && peerEpoch == other.peerEpoch);

	}

	@Override
	public int hashCode() {
		return (int) (id & txid);
	}

	public String toString() {
		return "(" + id + ", " + Long.toHexString(txid) + ", " + Long.toHexString(peerEpoch) + ")";
	}
}
