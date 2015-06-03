package com.alibaba.rocketmq.namesrv.paxos;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.common.ServiceThread;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.protocol.RequestCode;
import com.alibaba.rocketmq.common.protocol.ResponseCode;
import com.alibaba.rocketmq.common.protocol.body.LeaderElectionBody;
import com.alibaba.rocketmq.common.protocol.header.namesrv.LeaderElectionRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.namesrv.LeaderElectionRequestHeader.ServerState;
import com.alibaba.rocketmq.remoting.InvokeCallback;
import com.alibaba.rocketmq.remoting.RemotingClient;
import com.alibaba.rocketmq.remoting.exception.RemotingConnectException;
import com.alibaba.rocketmq.remoting.exception.RemotingSendRequestException;
import com.alibaba.rocketmq.remoting.exception.RemotingTimeoutException;
import com.alibaba.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import com.alibaba.rocketmq.remoting.netty.NettyClientConfig;
import com.alibaba.rocketmq.remoting.netty.NettyRemotingClient;
import com.alibaba.rocketmq.remoting.netty.ResponseFuture;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;

public class FastLeaderElection {
	private static final Logger LOG = LoggerFactory
			.getLogger(LoggerName.NamesrvLoggerName);

	volatile boolean stop;
	private final ArrayBlockingQueue<LeaderElectionRequestHeader> recvQueue = new ArrayBlockingQueue<LeaderElectionRequestHeader>(
			100);
	private LinkedBlockingQueue<RemotingCommand> sendqueue = new LinkedBlockingQueue<RemotingCommand>();
	private final WorkerReceiver workerReceiver;
	private AtomicLong logicalclock = new AtomicLong(0); /* Election instance */
	private ServerState state = ServerState.LOOKING;
	volatile private LeaderElectionRequestHeader currentVote;
	private long myid;
	private RemotingClient remotingClient;
	private String[] nsAddrs;

	public FastLeaderElection() {
		this.stop = false;
		workerReceiver = new WorkerReceiver();
		NettyClientConfig nettyClientConfig = new NettyClientConfig();
		remotingClient = new NettyRemotingClient(nettyClientConfig);
        //remotingClient.registerRPCHook(rpcHook);
        remotingClient.start();
	}

	public boolean putRequest(final LeaderElectionRequestHeader request) {
		if(recvQueue.offer(request)){
			workerReceiver.putRequest();
			return true;
		}
		return false;
	}
	
	synchronized public void startLeaderElection() throws RemotingConnectException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException, InterruptedException {
		
		currentVote = new LeaderElectionRequestHeader();
		currentVote.setState(state);
		currentVote.setSid(myid);
		currentVote.setLogicalclock(logicalclock.get());
		RemotingCommand request =
                RemotingCommand.createRequestCommand(RequestCode.UNREGISTER_BROKER, currentVote);
		if(currentVote.getBody()!=null){
			request.setBody(currentVote.getBody().encode());
		}
		for(String addr:nsAddrs){
			RemotingCommand response = remotingClient.invokeSync(addr, request, 3000);
			assert response != null;
	        switch (response.getCode()) {
	        case ResponseCode.SUCCESS: {
	            return;
	        }
	        default:
	            break;
	        }
		}
		synchronized (this) {
			logicalclock.incrementAndGet();
		}
	}

	class WorkerReceiver extends ServiceThread {

		WorkerReceiver() {
		}

		public void putRequest() {
			synchronized (this) {
				if (!this.hasNotified) {
					this.hasNotified = true;
					this.notify();

					// TODO 这里要Notify两个线程 1、GroupTransferService
					// 2、WriteSocketService
					// 在调用putRequest后，已经Notify了WriteSocketService
				}
			}
		}

		public void run() {
			LOG.info(this.getServiceName() + " service started");

			while (!this.isStoped()) {
				try {
					if (!this.process())
						this.waitForRunning(0);
				} catch (Exception e) {
					LOG.error(this.getServiceName()
							+ " service has exception. ", e);
				}
			}

			LOG.info(this.getServiceName() + " service end");
		}

		private boolean process() throws InterruptedException {
			LeaderElectionRequestHeader request = recvQueue.poll(3000,
					TimeUnit.MILLISECONDS);
			if (request == null)
				return false;

			LeaderElectionBody leaderElectionBody = request.getBody();
			try {

				ServerState rstate = request.getState();
				long rleader = request.getSid();
				long relectionEpoch = request.getLogicalclock();
				long rpeerepoch;

			} catch (Exception e) {
				LOG.warn("Interrupted Exception while waiting for new message"
						+ e.toString());
			}
			
			return true;
		}

		@Override
		public String getServiceName() {
			return WorkerReceiver.class.getSimpleName();
		}
	}

	HashMap<Long, LeaderElectionRequestHeader> recvset = new HashMap<Long, LeaderElectionRequestHeader>();
	HashMap<Long, LeaderElectionRequestHeader> outofelection = new HashMap<Long, LeaderElectionRequestHeader>();

	/**
	 * Starts a new round of leader election. Whenever our QuorumPeer changes
	 * its state to LOOKING, this method is invoked, and it sends notifications
	 * to all other peers.
	 */
	public void lookForLeader(LeaderElectionRequestHeader n) throws InterruptedException {
		/*
		 * try { self.jmxLeaderElectionBean = new LeaderElectionBean();
		 * MBeanRegistry.getInstance().register( self.jmxLeaderElectionBean,
		 * self.jmxLocalPeerBean); } catch (Exception e) {
		 * LOG.warn("Failed to register with JMX", e);
		 * self.jmxLeaderElectionBean = null; } if (self.start_fle == 0) {
		 * self.start_fle = System.currentTimeMillis(); }
		 */
		try {
			
			


			/*
			 * Loop in which we exchange notifications until we find a leader
			 */

			while ((state == ServerState.LOOKING) && (!stop)) {

				/*
				 * Sends more notifications if haven't received enough.
				 * Otherwise processes new notification.
				 */
				if (n == null) {

					/*
					 * Exponential backoff
					 */
					int tmpTimeOut = notTimeout * 2;
					notTimeout = (tmpTimeOut < maxNotificationInterval ? tmpTimeOut
							: maxNotificationInterval);
					LOG.info("Notification time out: " + notTimeout);
				} else if (self.getCurrentAndNextConfigVoters().contains(n.sid)) {
					/*
					 * Only proceed if the vote comes from a replica in the
					 * current or next voting view.
					 */
					switch (n.state) {
					case LOOKING:
						// If notification > current, replace and send messages
						// out
						if (n.electionEpoch > logicalclock.get()) {
							logicalclock.set(n.electionEpoch);
							recvset.clear();
							if (totalOrderPredicate(n.leader, n.zxid,
									n.peerEpoch, getInitId(),
									getInitLastLoggedZxid(), getPeerEpoch())) {
								updateProposal(n.leader, n.zxid, n.peerEpoch);
							} else {
								updateProposal(getInitId(),
										getInitLastLoggedZxid(), getPeerEpoch());
							}
							sendNotifications();
						} else if (n.electionEpoch < logicalclock.get()) {
							if (LOG.isDebugEnabled()) {
								LOG.debug("Notification election epoch is smaller than logicalclock. n.electionEpoch = 0x"
										+ Long.toHexString(n.electionEpoch)
										+ ", logicalclock=0x"
										+ Long.toHexString(logicalclock.get()));
							}
							break;
						} else if (totalOrderPredicate(n.leader, n.zxid,
								n.peerEpoch, proposedLeader, proposedZxid,
								proposedEpoch)) {
							updateProposal(n.leader, n.zxid, n.peerEpoch);
							sendNotifications();
						}

						if (LOG.isDebugEnabled()) {
							LOG.debug("Adding vote: from=" + n.sid
									+ ", proposed leader=" + n.leader
									+ ", proposed zxid=0x"
									+ Long.toHexString(n.zxid)
									+ ", proposed election epoch=0x"
									+ Long.toHexString(n.electionEpoch));
						}

						recvset.put(n.sid, new LeaderElectionRequestHeader(n.leader, n.zxid,
								n.electionEpoch, n.peerEpoch));

						if (termPredicate(recvset,
								new Vote(proposedLeader, proposedZxid,
										logicalclock.get(), proposedEpoch))) {

							// Verify if there is any change in the proposed
							// leader
							while ((n = recvqueue.poll(finalizeWait,
									TimeUnit.MILLISECONDS)) != null) {
								if (totalOrderPredicate(n.leader, n.zxid,
										n.peerEpoch, proposedLeader,
										proposedZxid, proposedEpoch)) {
									recvqueue.put(n);
									break;
								}
							}

							/*
							 * This predicate is true once we don't read any new
							 * relevant message from the reception queue
							 */
							if (n == null) {
								self.setPeerState((proposedLeader == self
										.getId()) ? ServerState.LEADING
										: learningState());

								Vote endVote = new Vote(proposedLeader,
										proposedZxid, proposedEpoch);
								leaveInstance(endVote);
								return endVote;
							}
						}
						break;
					case OBSERVING:
						LOG.debug("Notification from observer: " + n.sid);
						break;
					case FOLLOWING:
					case LEADING:
						/*
						 * Consider all notifications from the same epoch
						 * together.
						 */
						if (n.electionEpoch == logicalclock.get()) {
							recvset.put(n.sid, new Vote(n.leader, n.zxid,
									n.electionEpoch, n.peerEpoch));
							if (termPredicate(recvset, new Vote(n.leader,
									n.zxid, n.electionEpoch, n.peerEpoch,
									n.state))
									&& checkLeader(outofelection, n.leader,
											n.electionEpoch)) {
								self.setPeerState((n.leader == self.getId()) ? ServerState.LEADING
										: learningState());

								Vote endVote = new Vote(n.leader, n.zxid,
										n.peerEpoch);
								leaveInstance(endVote);
								return endVote;
							}
						}

						/*
						 * Before joining an established ensemble, verify that a
						 * majority are following the same leader. Only peer
						 * epoch is used to check that the votes come from the
						 * same ensemble. This is because there is at least one
						 * corner case in which the ensemble can be created with
						 * inconsistent zxid and election epoch info. However,
						 * given that only one ensemble can be running at a
						 * single point in time and that each epoch is used only
						 * once, using only the epoch to compare the votes is
						 * sufficient.
						 * 
						 * @see
						 * https://issues.apache.org/jira/browse/ZOOKEEPER-1732
						 */
						outofelection
								.put(n.sid, new Vote(n.leader, IGNOREVALUE,
										IGNOREVALUE, n.peerEpoch, n.state));
						if (termPredicate(outofelection, new Vote(n.leader,
								IGNOREVALUE, IGNOREVALUE, n.peerEpoch, n.state))
								&& checkLeader(outofelection, n.leader,
										IGNOREVALUE)) {
							synchronized (this) {
								logicalclock.set(n.electionEpoch);
								self.setPeerState((n.leader == self.getId()) ? ServerState.LEADING
										: learningState());
							}
							Vote endVote = new Vote(n.leader, n.zxid,
									n.peerEpoch);
							leaveInstance(endVote);
							return endVote;
						}
						break;
					default:
						LOG.warn("Notification state unrecoginized: " + n.state
								+ " (n.state), " + n.sid + " (n.sid)");
						break;
					}
				} else {
					LOG.warn("Ignoring notification from non-cluster member "
							+ n.sid);
				}
			}
			return null;
		} finally {
			try {
				if (self.jmxLeaderElectionBean != null) {
					MBeanRegistry.getInstance().unregister(
							self.jmxLeaderElectionBean);
				}
			} catch (Exception e) {
				LOG.warn("Failed to unregister with JMX", e);
			}
			self.jmxLeaderElectionBean = null;
		}
	}

	/**
	 * This worker simply dequeues a message to send and and queues it on the
	 * manager's queue.
	 */

	class WorkerSender extends ZooKeeperThread {
		volatile boolean stop;
		QuorumCnxManager manager;

		WorkerSender(QuorumCnxManager manager) {
			super("WorkerSender");
			this.stop = false;
			this.manager = manager;
		}

		public void run() {
			while (!stop) {
				try {
					ToSend m = sendqueue.poll(3000, TimeUnit.MILLISECONDS);
					if (m == null)
						continue;

					process(m);
				} catch (InterruptedException e) {
					break;
				}
			}
			LOG.info("WorkerSender is down");
		}

		/**
		 * Called by run() once there is a new message to send.
		 * 
		 * @param m
		 *            message to send
		 */
		void process(ToSend m) {
			ByteBuffer requestBuffer = buildMsg(m.state.ordinal(), m.leader,
					m.zxid, m.electionEpoch, m.peerEpoch, m.configData);

			manager.toSend(m.sid, requestBuffer);

		}
	}

	WorkerSender ws;
	WorkerReceiver wr;
	Thread wsThread = null;
	Thread wrThread = null;

	/**
	 * Constructor of class Messenger.
	 * 
	 * @param manager
	 *            Connection manager
	 */
	Messenger(QuorumCnxManager manager) {

		this.ws = new WorkerSender(manager);

		this.wsThread = new Thread(this.ws, "WorkerSender[myid=" + self.getId()
				+ "]");
		this.wsThread.setDaemon(true);

		this.wr = new WorkerReceiver(manager);

		this.wrThread = new Thread(this.wr, "WorkerReceiver[myid="
				+ self.getId() + "]");
		this.wrThread.setDaemon(true);
	}

	/**
	 * Starts instances of WorkerSender and WorkerReceiver
	 */
	void start() {
		this.wsThread.start();
		this.wrThread.start();
	}

	/**
	 * Stops instances of WorkerSender and WorkerReceiver
	 */
	void halt() {
		this.ws.stop = true;
		this.wr.stop = true;
	}

	static public class Notification {
		/*
		 * Format version, introduced in 3.4.6
		 */

		public final static int CURRENTVERSION = 0x2;
		int version;

		/*
		 * Proposed leader
		 */
		long leader;

		/*
		 * zxid of the proposed leader
		 */
		long zxid;

		/*
		 * Epoch
		 */
		long electionEpoch;

		/*
		 * current state of sender
		 */
		QuorumPeer.ServerState state;

		/*
		 * Address of sender
		 */
		long sid;

		QuorumVerifier qv;
		/*
		 * epoch of the proposed leader
		 */
		long peerEpoch;
	}

}
