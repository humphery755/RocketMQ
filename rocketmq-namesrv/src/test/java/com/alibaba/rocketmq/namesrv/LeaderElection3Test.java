package com.alibaba.rocketmq.namesrv;

public class LeaderElection3Test {

	public static void main(String[] args) {
		PaxosControllerTest p = new PaxosControllerTest("bin/mqnamesrv", "-c", "../conf/namesrv2.properties");
		try {
			p.setUp();
			p.test();
			p.tearDown();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
