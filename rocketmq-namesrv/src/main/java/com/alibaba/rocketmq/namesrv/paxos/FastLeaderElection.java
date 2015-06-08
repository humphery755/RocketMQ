package com.alibaba.rocketmq.namesrv.paxos;

import java.util.HashMap;
import java.util.Map;
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
import com.alibaba.rocketmq.common.protocol.header.namesrv.PaxosRequestHeader;
import com.alibaba.rocketmq.namesrv.PaxosController;
import com.alibaba.rocketmq.remoting.exception.RemotingConnectException;
import com.alibaba.rocketmq.remoting.exception.RemotingSendRequestException;
import com.alibaba.rocketmq.remoting.exception.RemotingTimeoutException;
import com.alibaba.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;

public class FastLeaderElection {
	private static final Logger LOG = LoggerFactory.getLogger(LoggerName.NamesrvLoggerName);
	final static int IGNOREVALUE = -1;

	private final PaxosController paxosController;
	//private final ArrayBlockingQueue<PaxosRequestHeader> recvQueue = new ArrayBlockingQueue<PaxosRequestHeader>(100);
	private final WorkerReceiver workerReceiver;
	private final WorkerSender workerSender;
	private AtomicLong logicalclock = new AtomicLong(0); /* Election instance */
	private int state = PaxosRequestHeader.LOOKING;
	//volatile private Vote currentVote;
	private long zxid = 0;
	private long leader = 0;
	private long electionEpoch;

	public enum LearnerType {
		PARTICIPANT, OBSERVER;
	}

	/*
	 * Default value of peer is participant
	 */
	private LearnerType learnerType = LearnerType.PARTICIPANT;

	public LearnerType getLearnerType() {
		return learnerType;
	}

	public FastLeaderElection(PaxosController paxosController) {
		this.paxosController = paxosController;
		workerReceiver = new WorkerReceiver();
		workerSender = new WorkerSender();
	}
	
	public void start() throws Exception {
		workerReceiver.start();
		workerSender.start();
		startLeaderElection();
	}

	public void shutdown() {
		workerReceiver.shutdown();
		workerSender.shutdown();
	}

	public boolean putRequest(final PaxosRequestHeader request) {
		/*if (recvQueue.offer(request)) {
			// workerReceiver.putRequest();
			return true;
		}*/
		Notification n = new Notification();
		n.sid = request.getSid();
		n.electionEpoch = request.getElectionEpoch();
		n.leader = request.getLeader();
		n.state = request.getState();
		workerReceiver.putRequest(n);
		return true;
	}

	synchronized public void startLeaderElection() throws RemotingConnectException, RemotingTooMuchRequestException, RemotingTimeoutException,
			RemotingSendRequestException, InterruptedException {
		synchronized (this) {
			logicalclock.incrementAndGet();
		}
		electionEpoch = paxosController.getMyid() * paxosController.getNsServers().size() + 1;
		//currentVote = new Vote(paxosController.getMyid(), electionEpoch);
		// workerReceiver.putRequest(currentVote);
		sendNotifications();
	}

	class WorkerReceiver extends ServiceThread {
		private final ArrayBlockingQueue<Notification> recvQueue = new ArrayBlockingQueue<Notification>(100);

		// 投票箱
		HashMap<Long, Vote> recvset = new HashMap<Long, Vote>();
		HashMap<Long, Vote> outofelection = new HashMap<Long, Vote>();
		
		WorkerReceiver() {
		}

		public void putRequest(final Notification request) {
			recvQueue.offer(request);
			synchronized (this) {
				if (!this.hasNotified) {
					this.hasNotified = true;
					this.notify();
				}
			}
		}

		public void run() {
			LOG.info(this.getServiceName() + " service started");

			while ((!isStoped())) {
				try {
					if (!this.process())
						this.waitForRunning(0);
				} catch (Exception e) {
					LOG.error(this.getServiceName() + " service has exception. ", e);
				}
			}

			LOG.info(this.getServiceName() + " service end");
		}

		private boolean process() throws InterruptedException {
			Notification n = recvQueue.poll(3000, TimeUnit.MILLISECONDS);
			if (n == null)
				return false;

			switch (n.state) {
			case PaxosRequestHeader.LOOKING:
				// If notification > current, replace and send messages
				// out
				if (n.electionEpoch > logicalclock.get()) {
					logicalclock.set(n.electionEpoch);
					recvset.clear();
					if (totalOrderPredicate(n.leader, n.zxid, n.electionEpoch, paxosController.getMyid(), zxid, electionEpoch)) {
						updateProposal(n.leader, n.zxid, n.electionEpoch);
					} else {
						updateProposal(paxosController.getMyid(), zxid, electionEpoch);
					}
					sendNotifications();
				} else if (n.electionEpoch < logicalclock.get()) {
					if (LOG.isDebugEnabled()) {
						LOG.debug("Notification election epoch is smaller than logicalclock. n.electionEpoch = 0x"
								+ Long.toHexString(n.electionEpoch) + ", logicalclock=0x" + Long.toHexString(logicalclock.get()));
					}
					break;
				} else if (totalOrderPredicate(n.leader, n.zxid, n.electionEpoch, leader, zxid, electionEpoch)) {
					updateProposal(n.leader, n.zxid, n.electionEpoch);
					sendNotifications();
				}

				recvset.put(n.sid, new Vote(n.leader, n.zxid, n.electionEpoch));

				if (termPredicate(recvset, new Vote(leader, zxid, logicalclock.get(), electionEpoch))) {

					// Verify if there is any change in the proposed
					// leader
					while ((n = recvQueue.poll(3000, TimeUnit.MILLISECONDS)) != null) {
						if (totalOrderPredicate(n.leader, n.zxid, n.electionEpoch, leader, zxid, electionEpoch)) {
							recvQueue.put(n);
							break;
						}
					}

					/*
					 * This predicate is true once we don't read any new
					 * relevant message from the reception queue
					 */
					if (n == null) {
						state = (leader == paxosController.getMyid()) ? PaxosRequestHeader.LEADING : learningState();

						Vote endVote = new Vote(leader, zxid, electionEpoch);
						recvQueue.clear();
						return true;// endVote;
					}
				}
				break;
			case PaxosRequestHeader.OBSERVING:
				LOG.debug("Notification from observer: " + n.sid);
				break;
			case PaxosRequestHeader.FOLLOWING:
			case PaxosRequestHeader.LEADING:
				/*
				 * Consider all notifications from the same epoch together.
				 */
				if (n.electionEpoch == logicalclock.get()) {
					Vote vote = new Vote(n.leader, n.zxid, n.electionEpoch, n.electionEpoch, n.state);
					recvset.put(n.sid, vote);
					if (termPredicate(recvset, vote) && checkLeader(outofelection, n.leader, n.electionEpoch)) {
						state = (n.leader == paxosController.getMyid()) ? PaxosRequestHeader.LEADING : learningState();

						Vote endVote = new Vote(n.leader, n.zxid, n.peerEpoch);
						recvQueue.clear();
						return true;// endVote;
					}
				}

				/*
				 * Before joining an established ensemble, verify that a
				 * majority are following the same leader. Only peer epoch is
				 * used to check that the votes come from the same ensemble.
				 * This is because there is at least one corner case in which
				 * the ensemble can be created with inconsistent zxid and
				 * election epoch info. However, given that only one ensemble
				 * can be running at a single point in time and that each epoch
				 * is used only once, using only the epoch to compare the votes
				 * is sufficient.
				 * 
				 * @see https://issues.apache.org/jira/browse/ZOOKEEPER-1732
				 */
				outofelection.put(n.sid, new Vote(n.leader, IGNOREVALUE, IGNOREVALUE, n.peerEpoch, n.state));
				if (termPredicate(outofelection, new Vote(n.leader, IGNOREVALUE, IGNOREVALUE, n.peerEpoch, n.state))
						&& checkLeader(outofelection, n.leader, IGNOREVALUE)) {
					synchronized (this) {
						logicalclock.set(n.electionEpoch);
						state = ((n.leader == paxosController.getMyid()) ? PaxosRequestHeader.LEADING : learningState());
					}
					// Vote endVote = new Vote(n.leader,
					// n.zxid,n.electionEpoch);
					recvQueue.clear();
					return true;// endVote;
				}
				break;
			default:
				LOG.warn("Notification state unrecoginized: " + n.state + " (n.state), " + n.sid + " (n.sid)");
				break;
			}

			return true;
		}

		@Override
		public String getServiceName() {
			return WorkerReceiver.class.getSimpleName();
		}
	}

	private int learningState() {
		if (getLearnerType() == LearnerType.PARTICIPANT) {
			LOG.debug("I'm a participant: " + paxosController.getMyid());
			return PaxosRequestHeader.FOLLOWING;
		} else {
			LOG.debug("I'm an observer: " + paxosController.getMyid());
			return PaxosRequestHeader.OBSERVING;
		}
	}

	/**
	 * In the case there is a leader elected, and a quorum supporting this
	 * leader, we have to check if the leader has voted and acked that it is
	 * leading. We need this check to avoid that peers keep electing over and
	 * over a peer that has crashed and it is no longer leading.
	 * 
	 * @param votes
	 *            set of votes
	 * @param leader
	 *            leader id
	 * @param electionEpoch
	 *            epoch id
	 */
	private boolean checkLeader(HashMap<Long, Vote> votes, long leader, long electionEpoch) {

		boolean predicate = true;

		/*
		 * If everyone else thinks I'm the leader, I must be the leader. The
		 * other two checks are just for the case in which I'm not the leader.
		 * If I'm not the leader and I haven't received a message from leader
		 * stating that it is leading, then predicate is false.
		 */

		if (leader != paxosController.getMyid()) {
			if (votes.get(leader) == null)
				predicate = false;
			else if (votes.get(leader).getState() != PaxosRequestHeader.LEADING)
				predicate = false;
		} else if (logicalclock.get() != electionEpoch) {
			predicate = false;
		}

		return predicate;
	}

	/**
	 * Termination predicate. Given a set of votes, determines if have
	 * sufficient to declare the end of the election round.
	 * 
	 * @param votes
	 *            Set of votes
	 * @param vote
	 *            Identifier of the vote received last
	 */
	private boolean termPredicate(HashMap<Long, Vote> votes, Vote vote) {

		/*
		 * First make the views consistent. Sometimes peers will have different
		 * zxids for a server depending on timing.
		 */
		int maxCount = paxosController.getAllNsAddrs().length/2+1;
		int count = 0;
		for (Map.Entry<Long, Vote> entry : votes.entrySet()) {
			if (vote.equals(entry.getValue())) {
				count++;
			}
		}

		return count >= maxCount;
	}

	private void updateProposal(long leader, long zxid, long epoch) {
		this.leader = leader;
		this.zxid = zxid;
		electionEpoch = epoch;
	}

	/**
	 * Check if a pair (server id, zxid) succeeds our current vote.
	 * 
	 * @param id
	 *            Server identifier
	 * @param zxid
	 *            Last zxid observed by the issuer of this vote
	 */
	protected boolean totalOrderPredicate(long newId, long newZxid, long newEpoch, long curId, long curZxid, long curEpoch) {
		LOG.debug("id: " + newId + ", proposed id: " + curId + ", zxid: 0x" + Long.toHexString(newZxid) + ", proposed zxid: 0x"
				+ Long.toHexString(curZxid));
		/*
		 * if(self.getQuorumVerifier().getWeight(newId) == 0){ return false; }
		 */

		/*
		 * We return true if one of the following three cases hold: 1- New epoch
		 * is higher 2- New epoch is the same as current epoch, but new zxid is
		 * higher 3- New epoch is the same as current epoch, new zxid is the
		 * same as current zxid, but server id is higher.
		 */

		return ((newEpoch > curEpoch) || ((newEpoch == curEpoch) && ((newZxid > curZxid) || ((newZxid == curZxid) && (newId > curId)))));
	}

	private void sendNotifications() {
		PaxosRequestHeader req = new PaxosRequestHeader();
		req.setElectionEpoch(electionEpoch);
		req.setLeader(leader);
		req.setZxid(zxid);
		req.setState(state);
		req.setSid(paxosController.getMyid());

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PAXOS_ALGORITHM_REQUEST_CODE, req);
		if (req.getBody() != null) {
			request.setBody(req.getBody().encode());
		}
		workerSender.putRequest(request);
		/*
		 * for (long sid : nsServers.keySet()) { QuorumVerifier qv =
		 * self.getQuorumVerifier(); ToSend notmsg = new
		 * ToSend(ToSend.mType.notification, proposedLeader, proposedZxid,
		 * logicalclock.get(), QuorumPeer.ServerState.LOOKING, sid,
		 * proposedEpoch, qv.toString().getBytes()); if(LOG.isDebugEnabled()){
		 * LOG.debug("Sending Notification: " + proposedLeader +
		 * " (n.leader), 0x" + Long.toHexString(proposedZxid) + " (n.zxid), 0x"
		 * + Long.toHexString(logicalclock.get()) + " (n.round), " + sid +
		 * " (recipient), " + self.getId() + " (paxosController.getMyid()), 0x"
		 * + Long.toHexString(proposedEpoch) + " (n.peerEpoch)"); }
		 * sendqueue.offer(currentVote); }
		 */
	}

	/**
	 * This worker simply dequeues a message to send and and queues it on the
	 * manager's queue.
	 */

	class WorkerSender extends ServiceThread {
		private LinkedBlockingQueue<RemotingCommand> sendqueue = new LinkedBlockingQueue<RemotingCommand>();

		WorkerSender() {
		}
		
		public void putRequest(final RemotingCommand request) {
			sendqueue.offer(request);
			synchronized (this) {
				if (!this.hasNotified) {
					this.hasNotified = true;
					this.notify();
				}
			}
		}

		public void run() {
			LOG.info(this.getServiceName() + " service started");

			while ((!isStoped())) {
				try {
					if (!this.process())
						this.waitForRunning(0);
				} catch (Exception e) {
					LOG.error(this.getServiceName() + " service has exception. ", e);
				}
			}

			LOG.info(this.getServiceName() + " service end");
		}

		/**
		 * Called by run() once there is a new message to send.
		 * 
		 * @param m
		 *            message to send
		 * @throws InterruptedException
		 * @throws RemotingTimeoutException
		 * @throws RemotingSendRequestException
		 * @throws RemotingConnectException
		 */
		boolean process() throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException,
				InterruptedException {
			RemotingCommand m = sendqueue.poll(3000, TimeUnit.MILLISECONDS);
			if (m == null)
				return false;
			for (String addr : paxosController.getAllNsAddrs()) {
				RemotingCommand response = paxosController.getRemotingClient().invokeSync(addr, m, 3000);
				assert response != null;
				switch (response.getCode()) {
				case ResponseCode.SUCCESS: {
					continue;
				}
				default:
					break;
				}
			}
			return true;
		}

		@Override
		public String getServiceName() {
			return WorkerSender.class.getSimpleName();
		}
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
		int state;

		/*
		 * Address of sender
		 */
		long sid;

		/*
		 * epoch of the proposed leader
		 */
		long peerEpoch;
	}
}
