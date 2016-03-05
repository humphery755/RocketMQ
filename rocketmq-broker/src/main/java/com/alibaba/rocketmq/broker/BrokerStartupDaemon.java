package com.alibaba.rocketmq.broker;

import java.io.IOException;

import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.daemon.DaemonController;

import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.common.protocol.RequestCode;
import com.alibaba.rocketmq.common.protocol.ResponseCode;
import com.alibaba.rocketmq.common.protocol.body.PaxosNotificationBody;
import com.alibaba.rocketmq.common.protocol.header.namesrv.PaxosRequestHeader;
import com.alibaba.rocketmq.remoting.RemotingClient;
import com.alibaba.rocketmq.remoting.exception.RemotingConnectException;
import com.alibaba.rocketmq.remoting.exception.RemotingSendRequestException;
import com.alibaba.rocketmq.remoting.exception.RemotingTimeoutException;
import com.alibaba.rocketmq.remoting.netty.NettyClientConfig;
import com.alibaba.rocketmq.remoting.netty.NettyRemotingClient;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;
import com.alibaba.rocketmq.store.config.BrokerRole;

public class BrokerStartupDaemon implements Daemon, Runnable {

	private Thread brokerThread = null;
	private Thread haThread = null;
	private DaemonController controller = null;
	private volatile boolean stopping = false;
	private String[] args;
	private final RemotingClient remotingClient;
	private static BrokerController brokerController;

	public static native void toto();

	public BrokerStartupDaemon() {
		super();
		System.err.println("BrokerStartupDaemon: instance " + this.hashCode() + " created");
		NettyClientConfig nettyClientConfig = new NettyClientConfig();
		this.remotingClient = new NettyRemotingClient(nettyClientConfig);
	}

	public long getMasterBroker(final String addr) throws InterruptedException, RemotingTimeoutException,
			RemotingSendRequestException, RemotingConnectException, MQBrokerException {
		PaxosRequestHeader req = new PaxosRequestHeader();
		req.setCode(PaxosRequestHeader.PAXOS_GET_MASTER_BROKER);
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PAXOS_ALGORITHM_REQUEST_CODE, req);

		RemotingCommand response = this.remotingClient.invokeSync(addr, request, 3000);
		response.getBody();

		assert response != null;
		switch (response.getCode()) {
		case ResponseCode.SUCCESS: {
			PaxosNotificationBody leaderElectionBody = PaxosNotificationBody.decode(response.getBody(),
					PaxosNotificationBody.class);
			return leaderElectionBody.getLeader();
		}
		default:
			break;
		}

		throw new MQBrokerException(response.getCode(), response.getRemark());
	}

	public PaxosRequestHeader getLeader(final String addr) throws InterruptedException, RemotingTimeoutException,
			RemotingSendRequestException, RemotingConnectException, MQBrokerException {
		PaxosRequestHeader req = new PaxosRequestHeader();
		req.setCode(PaxosRequestHeader.PAXOS_GET_LEADER_NODE);
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PAXOS_ALGORITHM_REQUEST_CODE, req);

		RemotingCommand response = this.remotingClient.invokeSync(addr, request, 3000);
		response.getBody();

		assert response != null;
		switch (response.getCode()) {
		case ResponseCode.SUCCESS: {
			PaxosNotificationBody body = PaxosNotificationBody.decode(response.getBody(),
					PaxosNotificationBody.class);
			req.setBody(body);
			return req;
		}
		default:
			break;
		}

		throw new MQBrokerException(response.getCode(), response.getRemark());
	}

	protected void finalize() {
		System.err.println("BrokerStartupDaemon: instance " + this.hashCode() + " garbage collected");
	}

	/**
	 * init and destroy were added in jakarta-tomcat-daemon.
	 */
	public void init(DaemonContext context) throws Exception {
		System.err.println("BrokerStartupDaemon: instance " + this.hashCode() + " init");

		args = context.getArguments();
		brokerController = BrokerStartup.createBrokerController(args);
		/* Set up this simple daemon */
		this.controller = context.getController();
		this.brokerThread = new Thread() {
			public void run() {
				BrokerStartup.start(brokerController);
			}
		};
		brokerThread.setDaemon(true);
		this.haThread = new Thread(this);
	}

	public void start() {
		/* Dump a message */
		System.err.println("BrokerStartupDaemon: starting");
		remotingClient.start();
		/* Start */
		this.haThread.start();
	}

	public void stop() throws IOException, InterruptedException {
		/* Dump a message */
		System.err.println("BrokerStartupDaemon: stopping");

		/* Close the ServerSocket. This will make our thread to terminate */
		this.stopping = true;

		/* Wait for the main thread to exit and dump a message */
		this.brokerThread.join(5000);
		remotingClient.shutdown();
		System.err.println("BrokerStartupDaemon: stopped");
	}

	public void destroy() {
		System.err.println("BrokerStartupDaemon: instance " + this.hashCode() + " destroy");
	}

	public void run() {

		System.err.println("BrokerStartupDaemon: started acceptor loop");
		try {
			while (!this.stopping) {
				PaxosRequestHeader req=getLeader(brokerController.getBrokerConfig().getNamesrvAddr());
				long leader = req.getBody().getLeader();
				String addr = req.getBody().getAddr();
				long masterId=getMasterBroker(addr);
				if(masterId==brokerController.getBrokerConfig().getBrokerId()){
					brokerController.getMessageStoreConfig().setBrokerRole(BrokerRole.ASYNC_MASTER);
					brokerController.start();
				}
			}
		} catch (Exception e) {
			/*
			 * Don't dump any error message if we are stopping. A IOException is
			 * generated when the ServerSocket is closed in stop()
			 */
			if (!this.stopping)
				e.printStackTrace(System.err);
		}

		System.err.println("BrokerStartupDaemon: exiting acceptor loop");
	}
	
	public static void main(String[] args) throws Exception {
		DaemonContext ctx=new DaemonContext(){
			public DaemonController getController() {return new DaemonController(){
					public void shutdown() throws IllegalStateException {}
					public void reload() throws IllegalStateException {}
					public void fail() throws IllegalStateException {}
					public void fail(String message) throws IllegalStateException {}
					public void fail(Exception exception) throws IllegalStateException {}
					public void fail(String message, Exception exception) throws IllegalStateException {}};		}
			public String[] getArguments() {	return new String[]{"bin/sss","-c","E:/humphery755/rocketmq-3.2.6/conf/2m-2s-async/broker-a.properties"};	}			
		};
		BrokerStartupDaemon daemon=new BrokerStartupDaemon();
		daemon.init(ctx);
		daemon.start();
		
    }
}
