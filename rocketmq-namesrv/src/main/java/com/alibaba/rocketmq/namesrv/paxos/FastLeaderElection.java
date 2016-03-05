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
import com.alibaba.rocketmq.common.protocol.body.PaxosNotificationBody;
import com.alibaba.rocketmq.common.protocol.header.namesrv.PaxosRequestHeader;
import com.alibaba.rocketmq.namesrv.PaxosController;
import com.alibaba.rocketmq.remoting.common.RemotingUtil;
import com.alibaba.rocketmq.remoting.exception.RemotingConnectException;
import com.alibaba.rocketmq.remoting.exception.RemotingSendRequestException;
import com.alibaba.rocketmq.remoting.exception.RemotingTimeoutException;
import com.alibaba.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;

/**
 *
 * 成为Leader的必要条件？ Leader要具有最高的txid；集群中大多数的机器（至少n/2 + 1）得到响应并follow选出的Leader。
 * 选举采用2PC： 1 prepared - 我选我通知，并将自己状态置为LOOKING.  phase 1 － 根据选举规则提交Leader,并奖自己状态置为FLLOWER/LEADER.  phase 2 - Leader发出结果公告
 * 
 * 如果所有txid都相同(例如: 刚初始化时),此时有可能不能形成n/2+1个Server，怎么办？
 * 每一个Server都有一个ID,这个ID是不重复的，如果遇到这样的情况时，
 * 就推荐ID最大的哪个Server作为Leader。
 * 
 *Fllower怎么知道Leader还存活？
 * Leader选举出来后，Fllower定时向Leader发ping消息，当发现Leader无法ping通时，就改变自己的状态(LOOKING)，发起新的一轮选举。
 * 
 * @author humphery
 *
 */
public class FastLeaderElection {
	private static final Logger LOG = LoggerFactory.getLogger(LoggerName.NamesrvLoggerName);
	final static int IGNOREVALUE = -1;

	private final PaxosController paxosController;
	private final WorkerReceiver workerReceiver;
	private final WorkerSender workerSender;
	private final LeaderCheck leaderCheck;
	private AtomicLong logicalclock = new AtomicLong(0); /* Election instance */
	private int state = PaxosRequestHeader.LOOKING;
	volatile private Vote currentVote;

	private long leader = -1;
	// 状态的每一次改变, 都对应着一个递增的Transaction id, 该id称为txid 创建任意节点, 或者更新任意节点的数据,
	// 或者删除任意节点
	private long txid;
	private long epoch;

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

	public long getLeader() {
		return leader;
	}

	public FastLeaderElection(PaxosController paxosController) {
		this.paxosController = paxosController;
		workerReceiver = new WorkerReceiver();
		workerSender = new WorkerSender();
		leaderCheck = new LeaderCheck();

		long txid = paxosController.getMyid() * paxosController.getNsServers().size() + logicalclock.get();
		updateProposal(paxosController.getMyid(), txid, 0l);
	}

	public void start() throws Exception {
		workerReceiver.start();
		workerSender.start();
		leaderCheck.start();
		startLeaderElection();
	}

	public void shutdown() {
		workerReceiver.shutdown();
		workerSender.shutdown();
	}

	public boolean putRequest(final PaxosRequestHeader request) {
		workerReceiver.putRequest(request);
		return true;
	}

	synchronized public void startLeaderElection() {
		logicalclock.incrementAndGet();
		long txid = paxosController.getMyid() * paxosController.getNsServers().size() + logicalclock.get();
		LOG.debug("id: " + paxosController.getMyid()  + ", epoch: " + logicalclock.get() + ", peer epoch: " + logicalclock.get() + ", txid: " + txid);
		updateProposal(paxosController.getMyid(), txid, logicalclock.get());
		sendNotifications();
	}

	protected boolean totalOrderPredicate1(long newId, long newtxid, long newEpoch, long curId, long curtxid, long curEpoch) {
		LOG.debug("id: " + newId + ", proposed id: " + curId + ", txid: 0x" + Long.toHexString(newtxid) + ", proposed txid: 0x" + Long.toHexString(curtxid));
		return ((newEpoch == curEpoch) && ((newtxid < curtxid)) && ((newId < curId)));
	}

	class WorkerReceiver extends ServiceThread {
		private final ArrayBlockingQueue<PaxosRequestHeader> recvQueue = new ArrayBlockingQueue<PaxosRequestHeader>(100);

		// 投票箱
		HashMap<Long, Vote> recvset = new HashMap<Long, Vote>();
		HashMap<Long, Vote> outofelection = new HashMap<Long, Vote>();

		WorkerReceiver() {
		}

		public void putOutofelection(Vote v) {
			outofelection.put(v.getId(), v);
		}

		public void putRequest(final PaxosRequestHeader request) {
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
			PaxosRequestHeader n = recvQueue.poll(3000, TimeUnit.MILLISECONDS);
			if (n == null)
				return false;
			PaxosNotificationBody nb = n.getBody();
			if(nb.getState()==PaxosRequestHeader.RESET){
				if(nb.getElectionEpoch()<= logicalclock.get() && (FastLeaderElection.this.state==PaxosRequestHeader.FOLLOWING ||FastLeaderElection.this.state==PaxosRequestHeader.LEADING)){
					FastLeaderElection.this.leader = -1;
					FastLeaderElection.this.txid = -1;
					FastLeaderElection.this.epoch = -1;
					FastLeaderElection.this.state = PaxosRequestHeader.LOOKING;
					recvset.clear();
					outofelection.clear();
					startLeaderElection();
				}
				return true;
			}
			switch (state) {
			case PaxosRequestHeader.LOOKING:
				switch (nb.getState()) {
				case PaxosRequestHeader.LOOKING:

					// If notification > current, replace and send messages out
					if (nb.getElectionEpoch() > logicalclock.get()) {
						logicalclock.set(nb.getElectionEpoch());
						recvset.clear();
						if (totalOrderPredicate(nb.getLeader(), nb.getTxid(), nb.getElectionEpoch(), paxosController.getMyid(), currentVote.getTxid(), currentVote.getElectionEpoch())) {
							updateProposal(nb.getLeader(), nb.getTxid(), nb.getPeerEpoch());
						} else {
							updateProposal(paxosController.getMyid(), currentVote.getTxid(), currentVote.getPeerEpoch());
						}
						sendNotifications();
					} else if (nb.getElectionEpoch() < logicalclock.get()) {
						if (LOG.isDebugEnabled()) {
							LOG.debug("Notification election epoch is smaller than logicalclock. n.electionEpoch = 0x" + Long.toHexString(nb.getElectionEpoch()) + ", logicalclock=0x"
									+ Long.toHexString(logicalclock.get()));
						}
						sendNotification(n.getSid());
						break;
					} else if (totalOrderPredicate(nb.getLeader(), nb.getTxid(), nb.getElectionEpoch(), currentVote.getId(), currentVote.getTxid(), currentVote.getElectionEpoch())) {
						updateProposal(nb.getLeader(), nb.getTxid(), nb.getPeerEpoch());
						sendNotifications();
					}else if (currentVote.getTxid()>nb.getTxid()){
						sendNotifications();
					}
					recvset.put(n.getSid(), new Vote(nb.getLeader(), nb.getTxid(), nb.getElectionEpoch(), nb.getPeerEpoch()));

					if (termPredicate(recvset, new Vote(currentVote.getId(), currentVote.getTxid(), logicalclock.get(), currentVote.getPeerEpoch()))) {

						// Verify if there is any change in the proposed
						// leader
						while ((n = recvQueue.poll(3000, TimeUnit.MILLISECONDS)) != null) {
							if (totalOrderPredicate(nb.getLeader(), nb.getTxid(), nb.getElectionEpoch(), currentVote.getId(), currentVote.getTxid(), currentVote.getElectionEpoch())) {
								recvQueue.put(n);
								break;
							}
						}

						/*
						 * This predicate is true once we don't read any new
						 * relevant message from the reception queue
						 */
						if (n == null) {
							state = (currentVote.getId() == paxosController.getMyid()) ? PaxosRequestHeader.LEADING : learningState();
							currentVote = new Vote(currentVote.getId(), currentVote.getTxid(), currentVote.getElectionEpoch(), currentVote.getPeerEpoch(), state);
							recvQueue.clear();
							recvQueue.add(buildNotification());
							sendNotifications();
							break;// endVote;
						}
					}
					break;
				case PaxosRequestHeader.OBSERVING:
					LOG.debug("Notification from observer: " + n.getSid());
					break;
				case PaxosRequestHeader.FOLLOWING:
					LOG.debug("Notification from following: " + n.getSid());
					break;
				case PaxosRequestHeader.LEADING:
					if (nb.getElectionEpoch() > logicalclock.get() && n.getSid()==nb.getLeader()) {// leader already exists.
						logicalclock.set(nb.getElectionEpoch());
						recvset.clear();
						currentVote = new Vote(nb.getLeader(), nb.getTxid(), nb.getElectionEpoch(), nb.getPeerEpoch(), PaxosRequestHeader.FOLLOWING);
						leaderCheck.putRequest(nb);
						break;
					} 
					/*
					 * Consider all notifications from the same epoch together.
					 */

					if (nb.getElectionEpoch() == logicalclock.get()) {
						Vote vote = new Vote(nb.getLeader(), nb.getTxid(), nb.getElectionEpoch(), nb.getPeerEpoch(), nb.getState());
						recvset.put(n.getSid(), vote);
						if (termPredicate(recvset, vote) && checkLeader(outofelection, nb.getLeader(), nb.getElectionEpoch())) {
							state = (nb.getLeader() == paxosController.getMyid()) ? PaxosRequestHeader.LEADING : learningState();
							currentVote = new Vote(nb.getLeader(), nb.getTxid(), nb.getElectionEpoch(), nb.getPeerEpoch(), state);
							recvQueue.clear();
							sendNotifications();
							break;// endVote;
						}
					}
					
					outofelection.put(n.getSid(), new Vote(nb.getLeader(), IGNOREVALUE, IGNOREVALUE, nb.getPeerEpoch(), nb.getState()));
					if (termPredicate(outofelection, new Vote(nb.getLeader(), IGNOREVALUE, IGNOREVALUE, nb.getPeerEpoch(), nb.getState())) && checkLeader(outofelection, nb.getLeader(), IGNOREVALUE)) {
						synchronized (this) {
							logicalclock.set(nb.getElectionEpoch());
							state = ((nb.getLeader() == paxosController.getMyid()) ? PaxosRequestHeader.LEADING : learningState());
						}
						currentVote = new Vote(nb.getLeader(), nb.getTxid(), nb.getElectionEpoch(), nb.getPeerEpoch(), state);
						recvQueue.clear();
						sendNotifications();
						break;// endVote;
					}
					break;
				default:
					LOG.warn("Notification state unrecoginized: " + nb.getState() + " (n.state), " + n.getSid() + " (n.sid)");
					break;
				}
				break;
			case PaxosRequestHeader.OBSERVING:
				LOG.debug("Notification from observer: " + n.getSid());
				break;
			case PaxosRequestHeader.FOLLOWING:
			case PaxosRequestHeader.LEADING:
				if (leader != -1) {
					if (n.getSid() != paxosController.getMyid() && leader==paxosController.getMyid()) {
						sendNotification(n.getSid());
					}
					break;
				}
				switch (nb.getState()) {
				case PaxosRequestHeader.LOOKING:
					sendNotifications();
					break;
				case PaxosRequestHeader.OBSERVING:
					LOG.debug("Notification from observer: " + n.getSid());
					break;
				case PaxosRequestHeader.FOLLOWING:
				case PaxosRequestHeader.LEADING:
					/*
					 * Consider all notifications from the same epoch together.
					 */
					
					if (nb.getElectionEpoch() == logicalclock.get()) {
						Vote vote = new Vote(nb.getLeader(), nb.getTxid(), nb.getElectionEpoch(), nb.getPeerEpoch(), nb.getState());
						recvset.put(n.getSid(), vote);
						if (termPredicate(recvset, vote) && checkLeader(outofelection, nb.getLeader(), nb.getElectionEpoch())) {
							state = (nb.getLeader() == paxosController.getMyid()) ? PaxosRequestHeader.LEADING : learningState();
							recvQueue.clear();
							outofelection.clear();
							recvset.clear();
							// leader确定，更新epoch,重新计算txid，并公布结果
							FastLeaderElection.this.logicalclock.incrementAndGet();
							FastLeaderElection.this.txid = paxosController.getMyid() * paxosController.getNsServers().size() + logicalclock.get();
							FastLeaderElection.this.currentVote = new Vote(nb.getLeader(), txid, logicalclock.get(), logicalclock.get(), state);
							FastLeaderElection.this.leader = nb.getLeader();
							FastLeaderElection.this.epoch = currentVote.getElectionEpoch();
							LOG.info("Leader Election success, leader is {}", leader);
							sendNotifications();
							break;
						}
					}

					outofelection.put(n.getSid(), new Vote(nb.getLeader(), IGNOREVALUE, IGNOREVALUE, nb.getPeerEpoch(), nb.getState()));
					//termPredicate(outofelection, new Vote(nb.getLeader(), IGNOREVALUE, IGNOREVALUE, nb.getPeerEpoch(), nb.getState())
					if (nb.getElectionEpoch() > logicalclock.get() && checkLeader(outofelection, nb.getLeader(), IGNOREVALUE)) {
							FastLeaderElection.this.logicalclock.set(nb.getElectionEpoch());
							FastLeaderElection.this.state = ((nb.getLeader() == paxosController.getMyid()) ? PaxosRequestHeader.LEADING : learningState());
							recvQueue.clear();
							outofelection.clear();
							recvset.clear();
							leaderCheck.putRequest(nb);
						break;// endVote;
					}
					break;
				default:
					LOG.warn("Notification state unrecoginized: " + nb.getState() + " (n.state), " + n.getSid() + " (n.sid)");
					break;
				}
				break;
			default:
				LOG.warn("Notification state unrecoginized: " + nb.getState() + " (n.state), " + n.getSid() + " (n.sid)");
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
		int maxCount = paxosController.getNsServers().size() / 2 + 1;
		int count = 0;
		for (Map.Entry<Long, Vote> entry : votes.entrySet()) {
			if (vote.equals(entry.getValue())) {
				count++;
			}
		}

		return count >= maxCount;
	}

	private void updateProposal(long leader, long txid, long epoch) {
		currentVote = new Vote(leader, txid, epoch, epoch);
		workerReceiver.recvset.put(paxosController.getMyid(), currentVote);
	}

	/**
	 * Check if a pair (server id, txid) succeeds our current vote.
	 * 
	 * @param id
	 *            Server identifier
	 * @param txid
	 *            Last txid observed by the issuer of this vote
	 */
	protected boolean totalOrderPredicate(long newId, long newtxid, long newEpoch, long curId, long curtxid, long curEpoch) {
		LOG.debug("id: " + newId + ", proposed id: " + curId + ", txid: 0x" + Long.toHexString(newtxid) + ", proposed txid: 0x" + Long.toHexString(curtxid));
		return ((newEpoch > curEpoch) || ((newEpoch == curEpoch) && ((newtxid > curtxid) || ((newtxid == curtxid) && (newId > curId)))));
	}

	private PaxosRequestHeader buildNotification() {
		PaxosRequestHeader req = new PaxosRequestHeader();
		req.setBody(new PaxosNotificationBody());
		req.getBody().setElectionEpoch(currentVote.getElectionEpoch());
		req.getBody().setPeerEpoch(currentVote.getPeerEpoch());
		req.getBody().setLeader(currentVote.getId());
		req.getBody().setTxid(currentVote.getTxid());
		req.getBody().setState(currentVote.getState());
		req.setSid(paxosController.getMyid());
		return req;
	}

	private void sendNotifications() {
		PaxosRequestHeader req = buildNotification();
		// workerReceiver.putOutofelection(currentVote);
		workerSender.putRequest(req);
	}
	private void sendNotification(long sid) {
		PaxosRequestHeader req = buildNotification();
		Server s = paxosController.getNsServers().get(sid);
		// workerReceiver.putOutofelection(currentVote);
		req.getBody().setAddr(s.getAddr());
		workerSender.putRequest(req);
	}

	/**
	 * This worker simply dequeues a message to send and and queues it on the
	 * manager's queue.
	 */

	class WorkerSender extends ServiceThread {
		private LinkedBlockingQueue<PaxosRequestHeader> sendqueue = new LinkedBlockingQueue<PaxosRequestHeader>();
		private FailSendQueue failSendQueue = new FailSendQueue();

		public void start() {
			failSendQueue.start();
			super.start();
		}

		public void putRequest(final PaxosRequestHeader request) {
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
		 * @throws RemotingTooMuchRequestException
		 */
		boolean process() throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, RemotingTooMuchRequestException {
			PaxosRequestHeader req = sendqueue.poll(3000, TimeUnit.MILLISECONDS);
			if (req == null)
				return false;
			String addr=req.getBody().getAddr();
			req.getBody().setAddr(null);
			RemotingCommand m = RemotingCommand.createRequestCommand(RequestCode.PAXOS_ALGORITHM_REQUEST_CODE, req);
			m.setBody(req.getBody().encode());
			if(addr!=null){
				try {
					paxosController.getRemotingClient().invokeOneway(addr, m, 3000);
					failSendQueue.remove(m, addr);
				} catch (Exception e) {
					LOG.error(this.getServiceName() + " service has exception {}. ", e.getMessage());
					// failSendQueue.putRequest(m, s.getAddr());
				}
			}else{
				for (Server s : paxosController.getNsServers().values()) {
					if (s.getMyid() == paxosController.getMyid())
						continue;
					try {
						paxosController.getRemotingClient().invokeOneway(s.getAddr(), m, 3000);
						failSendQueue.remove(m, s.getAddr());
					} catch (Exception e) {
						LOG.error(this.getServiceName() + " service has exception {}. ", e.getMessage());
						// failSendQueue.putRequest(m, s.getAddr());
					}
				}
			}
			return true;
		}

		@Override
		public String getServiceName() {
			return WorkerSender.class.getSimpleName();
		}
	}

	// 选举消息（最后一次消息）失败重试
	class FailSendQueue extends ServiceThread {
		private final Map<String, Object[]> failMap = new HashMap();

		public void putRequest(RemotingCommand req, String addr) {
			PaxosRequestHeader h = (PaxosRequestHeader) req.readCustomHeader();
			synchronized (this) {
				failMap.put(addr + "-" + h.getSid(), new Object[] { addr, req, System.currentTimeMillis() });
			}
		}

		public void remove(RemotingCommand req, String addr) {
			PaxosRequestHeader h = (PaxosRequestHeader) req.readCustomHeader();
			synchronized (this) {
				failMap.remove(addr + "-" + h.getSid());
			}
		}

		public void run() {
			LOG.info(this.getServiceName() + " service started");

			while ((!isStoped())) {
				try {
					this.process();
					this.waitForRunning(10);
				} catch (Exception e) {
					LOG.error(this.getServiceName() + " service has exception. ", e);
				}
			}

			LOG.info(this.getServiceName() + " service end");
		}

		boolean process() throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, RemotingTooMuchRequestException {
			Map<String, Object[]> swapFailMap = new HashMap();
			swapFailMap.putAll(failMap);

			for (Map.Entry<String, Object[]> obj : swapFailMap.entrySet()) {
				String addr = (String) obj.getValue()[0];
				RemotingCommand m = (RemotingCommand) obj.getValue()[1];
				Long time = (Long) obj.getValue()[2];
				if (System.currentTimeMillis() - time < 60000)
					continue;
				PaxosRequestHeader h = (PaxosRequestHeader) m.readCustomHeader();
				try {
					paxosController.getRemotingClient().invokeOneway(addr, m, 3000);
					remove(m, addr);
				} catch (Exception e) {
					LOG.error(this.getServiceName() + " service has exception. {}", e.getMessage());
					remove(m, addr);
					putRequest(m, addr);
				}
			}

			return true;
		}

		@Override
		public String getServiceName() {
			return FailSendQueue.class.getSimpleName();
		}
	}

	/**
	 * 检测leader节点状态，失败则发起重新选举
	 * 
	 * @author humphery
	 *
	 */
	class LeaderCheck extends ServiceThread {
		private long leader = -1;
		private long epoch=-1;
		final RemotingCommand request;
		long interval = 0;
		int retrys = -1;

		public LeaderCheck() {
			PaxosRequestHeader rHeader = new PaxosRequestHeader();
			rHeader.setSid(paxosController.getMyid());
			rHeader.setCode(PaxosRequestHeader.PAXOS_PING_REQUEST_CODE);
			request = RemotingCommand.createRequestCommand(RequestCode.PAXOS_ALGORITHM_REQUEST_CODE, rHeader);
			PaxosNotificationBody body = new PaxosNotificationBody();
			body.setAddr(RemotingUtil.getLocalAddress() + ":" + paxosController.getNettyServerConfig().getListenPort());
			request.setBody(body.encode());
		}

		public void putRequest(PaxosNotificationBody nb) {
			synchronized (this) {
				FastLeaderElection.this.leader = this.leader = nb.getLeader();
				FastLeaderElection.this.txid = nb.getTxid();
				FastLeaderElection.this.epoch = epoch = nb.getElectionEpoch();
				FastLeaderElection.this.state=PaxosRequestHeader.FOLLOWING;
				interval = 30 * 1000;
				retrys = 3;
				if (!this.hasNotified) {
					this.hasNotified = true;
					this.notify();
				}
			}
		}

		public void remove() {
			this.leader = -1;
			this.epoch=-1;
			this.interval = 0;
		}

		public void run() {
			LOG.info(this.getServiceName() + " service started");

			while ((!isStoped())) {
				try {
					this.process();
					this.waitForRunning(interval);
				} catch (Exception e) {
					LOG.error(this.getServiceName() + " service has exception. ", e.getMessage());
					reset();
				}
			}

			LOG.info(this.getServiceName() + " service end");
		}

		private void reset() {
			retrys--;
			if (retrys <= 0) {
				synchronized (this) {
					if(this.leader==-1)return;
					PaxosRequestHeader req = new PaxosRequestHeader();
					req.setBody(new PaxosNotificationBody());
					req.getBody().setState(PaxosRequestHeader.RESET);
					req.getBody().setElectionEpoch(epoch);
					req.setSid(paxosController.getMyid());
					remove();
					workerReceiver.putRequest(req);
				}
			}
		}

		boolean process() throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, RemotingTooMuchRequestException {
			Server s = paxosController.getNsServers().get(leader);
			if (s == null) {
				return false;
			}

			paxosController.getRemotingClient().invokeOneway(s.getAddr(), request, 3000);

			return true;
		}

		@Override
		public String getServiceName() {
			return LeaderCheck.class.getSimpleName();
		}
	}

}
