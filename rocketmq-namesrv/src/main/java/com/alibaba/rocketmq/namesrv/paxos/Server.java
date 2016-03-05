package com.alibaba.rocketmq.namesrv.paxos;

public class Server {
	public static enum State {
		ACTIVE, DOWN
	}

	public Server(long id, String addr) {
		myid = id;
		this.addr = addr;
	}

	private long myid;
	private String addr;
	private State state = State.DOWN;

	public State getState() {
		return state;
	}

	public void setState(State state) {
		this.state = state;
	}

	public long getMyid() {
		return myid;
	}

	public String getAddr() {
		return addr;
	}

}
