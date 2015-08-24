package org.dna.mqtt.moquette.messaging.spi.impl;

import static org.dna.mqtt.moquette.parser.netty.Utils.VERSION_3_1;
import static org.dna.mqtt.moquette.parser.netty.Utils.VERSION_3_1_1;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.StringUtils;
import org.dna.mqtt.commons.ObjectUtils;
import org.dna.mqtt.moquette.messaging.spi.IMessagesStore;
import org.dna.mqtt.moquette.messaging.spi.ISessionsStore;
import org.dna.mqtt.moquette.messaging.spi.impl.events.MessagingEvent;
import org.dna.mqtt.moquette.messaging.spi.impl.events.OutputMessagingEvent;
import org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.Subscription;
import org.dna.mqtt.moquette.messaging.spi.impl.subscriptions.SubscriptionsStore;
import org.dna.mqtt.moquette.proto.messages.ConnAckMessage;
import org.dna.mqtt.moquette.proto.messages.ConnectMessage;
import org.dna.mqtt.moquette.proto.messages.DisconnectMessage;
import org.dna.mqtt.moquette.proto.messages.MessageType;
import org.dna.mqtt.moquette.proto.messages.PubAckMessage;
import org.dna.mqtt.moquette.proto.messages.PubCompMessage;
import org.dna.mqtt.moquette.proto.messages.PubRecMessage;
import org.dna.mqtt.moquette.proto.messages.PubRelMessage;
import org.dna.mqtt.moquette.proto.messages.PublishMessage;
import org.dna.mqtt.moquette.proto.messages.SubAckMessage;
import org.dna.mqtt.moquette.proto.messages.SubscribeMessage;
import org.dna.mqtt.moquette.proto.messages.UnsubAckMessage;
import org.dna.mqtt.moquette.proto.messages.UnsubscribeMessage;
import org.dna.mqtt.moquette.server.ConnectionDescriptor;
import org.dna.mqtt.moquette.server.Constants;
import org.dna.mqtt.moquette.server.ServerChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendCallback;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.taobao.tair.DataEntry;
import com.taobao.tair.Result;
import com.taobao.tair.ResultCode;
import com.taobao.tair.TairManager;
import com.taobao.tair.impl.DefaultTairManager;

public class ProtocolProcessor2MQ implements EventHandler<ValueEvent> {
	private static final Logger LOG = LoggerFactory.getLogger(ProtocolProcessor2MQ.class);
	// private ObjectPool<OutputMessagingEvent> pool = ;
	private RingBuffer<ValueEvent> m_ringBuffer;
	private ExecutorService m_executor;
	private final Map<String, ConnectionDescriptor> m_clientIDs = new ConcurrentHashMap<String, ConnectionDescriptor>();
	private final ConcurrentHashMap<String/* topic */, ConcurrentHashMap<String/* clientId */, Subscription>> subscriptionMap = new ConcurrentHashMap();
	private DefaultMQProducer mqProducer;
	private DefaultMQPushConsumer mqConsumer;
	private TairManager tairManager;
	private Integer myid;//节点ID，全局唯一
	private Integer brokergid;//broker集群标识
	private String CURRENT_TOPIC_UPG;

	private final static AtomicInteger at_pub_count = new AtomicInteger(0);
	private final static AtomicInteger at_sub_count = new AtomicInteger(0);

	void init(SubscriptionsStore subscriptions, IMessagesStore storageService, ISessionsStore sessionsStore, Properties configProps) {
		// m_clientIDs = clientIDs;
		LOG.debug("configProps on init {}", configProps.toString());

		myid = Integer.valueOf(configProps.getProperty("myid"));
		brokergid = Integer.valueOf(configProps.getProperty("brokergid"));
		int ringBufferSize = Integer.valueOf(configProps.getProperty("ringBufferSize.protocol.processor"));
		String strTairServList = configProps.getProperty("tair.confServList");
		String strTairGName = configProps.getProperty("tair.group");

		// init the output ringbuffer
		ThreadFactory disruptorThreadFactory = new ThreadFactory() {
			private AtomicInteger threadIndex = new AtomicInteger(0);

			@Override
			public Thread newThread(Runnable r) {
				return new Thread(r, String.format("ProtocolProcessor.Disruptor.executor%d", this.threadIndex.incrementAndGet()));
			}
		};
		m_executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() + 1, disruptorThreadFactory);
		//
		Disruptor<ValueEvent> disruptor = new Disruptor<ValueEvent>(ValueEvent.EVENT_FACTORY, 1024 * ringBufferSize, m_executor);
		disruptor.handleEventsWith(this);
		disruptor.start();

		// Get the ring buffer from the Disruptor to be used for publishing.
		m_ringBuffer = disruptor.getRingBuffer();

		DefaultTairManager _tairManager = new DefaultTairManager();
		List confServList = new ArrayList();
		String[] servList = StringUtils.split(strTairServList, ";");
		for (String serv : servList)
			confServList.add(serv);
		_tairManager.setConfigServerList(confServList);
		_tairManager.setGroupName(strTairGName);

		CURRENT_TOPIC_UPG = String.format(Constants.TOPIC_UPG_PATTERN, myid);
		final String CURRENT_TOPIC_DOWN = String.format(Constants.TOPIC_DOWNG_PATTERN, brokergid);
		final String CURRENT_TOPIC_SYS = String.format(Constants.TOPIC_SYS_PATTERN, brokergid);

		mqProducer = new DefaultMQProducer(String.format("MQTT_UP_PG%d", myid));
		mqConsumer = new DefaultMQPushConsumer(String.format("MQTT_DOWN_CG%d", myid));

		try {
			_tairManager.init();
			tairManager = _tairManager;

			mqProducer.start();

			mqConsumer.subscribe(CURRENT_TOPIC_SYS, "*");
			mqConsumer.subscribe(CURRENT_TOPIC_DOWN, "*");

			mqConsumer.registerMessageListener(new MessageListenerConcurrently() {

				@Override
				public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
					Set<String> cidSet = new HashSet();
					for (MessageExt me : msgs) {
						MQMessage mqMessage = ObjectUtils.deserialize(me.getBody(), MQMessage.class);
						if (CURRENT_TOPIC_SYS.equals(me.getTopic())) {
							if (mqMessage.getMessage() instanceof PublishMessageExt) {
								PublishMessageExt m = (PublishMessageExt) mqMessage.getMessage();
								PublishMessage pm = ext2PublishMessage(m);
								for (Map.Entry<String, ConnectionDescriptor> entry : m_clientIDs.entrySet()) {
									disruptorPublish(new OutputMessagingEvent(entry.getValue().getSession(), pm));
								}
							}
						} else if (CURRENT_TOPIC_DOWN.equals(me.getTopic())) {
							if (mqMessage.getMessage() instanceof PublishMessageExt) {
								PublishMessageExt m = (PublishMessageExt) mqMessage.getMessage();
								LOG.debug("SRV <--PUBLISH-- BATCH consumeMessage invoked for clientID {} ad messageID {}", me.getTags(),
										m.getMessageID());
								String tag = me.getTags();
								if (StringUtils.startsWith(tag, Constants.TOPIC_GROUP_PREFIX)) {
									//Map<String/* clientId */, Subscription> subMap = subscriptionMap.get(tag);
									cidSet.clear();
									cidSet = findClientsByTopic(tag,cidSet);
									if (!cidSet.isEmpty()) {
										
										for (String cid : cidSet) {
											//pm.getPayload().rewind();
											PublishMessage pm = ext2PublishMessage(m);
											sendPublish(cid, pm);
											//pm.setPayload(ByteBuffer.wrap(m.getPayload()));
											
										}
									}
								} else if (StringUtils.startsWith(tag, Constants.TOPIC_SYS_PREFIX)) {
									PublishMessage pm = ext2PublishMessage(m);
									for (Map.Entry<String, ConnectionDescriptor> entry : m_clientIDs.entrySet()) {
										disruptorPublish(new OutputMessagingEvent(entry.getValue().getSession(), pm));
									}
								} else if (StringUtils.startsWith(tag, Constants.TOPIC_PERSONAL_PREFIX)) {
									String cId = me.getTags();// StringUtils.substring(me.getTags(),
																// 4);
									PublishMessage pm = ext2PublishMessage(m);
									sendPublish(cId, pm);
								}

							} else if (mqMessage.getMessage() instanceof PubCompMessage) {
								PubCompMessage m = (PubCompMessage) mqMessage.getMessage();
								LOG.trace("SRV <--PUBCOMP-- BATCH consumeMessage invoked for clientID {} ad messageID {}", me.getTags(),
										m.getMessageID());
								String tag = me.getTags();
								if (StringUtils.startsWith(tag, Constants.TOPIC_GROUP_PREFIX)) {
									//Map<String/* clientId */, Subscription> subMap = subscriptionMap.get(tag);
									cidSet.clear();
									cidSet = findClientsByTopic(tag,cidSet);
									if (!cidSet.isEmpty()) {
										for (String cid : cidSet) {
											ConnectionDescriptor cd = m_clientIDs.get(me.getTags());
											if (cd == null)
												continue;
											disruptorPublish(new OutputMessagingEvent(cd.getSession(), m));
										}
									}
								} else if (StringUtils.startsWith(tag, Constants.TOPIC_SYS_PREFIX)) {
									for (Map.Entry<String, ConnectionDescriptor> entry : m_clientIDs.entrySet()) {
										disruptorPublish(new OutputMessagingEvent(entry.getValue().getSession(), m));
									}
								} else if (StringUtils.startsWith(tag, Constants.TOPIC_PERSONAL_PREFIX)) {
									String cId = me.getTags();// StringUtils.substring(me.getTags(),
																// 4);
									ConnectionDescriptor cd = m_clientIDs.get(cId);
									if (cd == null)
										continue;
									disruptorPublish(new OutputMessagingEvent(cd.getSession(), m));
								}
							}
						}
					}
					// return ConsumeConcurrentlyStatus.RECONSUME_LATER;
					return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
				}
			});

			mqConsumer.start();
		} catch (Exception e) {
			LOG.error("", e);
			System.exit(1);
		}

		ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);

		executor.scheduleAtFixedRate(new Runnable() {

			@Override
			public void run() {
				LOG.warn("Publish count:\t{}, Subscribe count:\t{}, Connection:\t{}, RingBuffer cur:\t{}", at_pub_count, at_sub_count,
						m_clientIDs.size(), m_ringBuffer.getCursor());
			}
		}, 10000, 10000, TimeUnit.MILLISECONDS);
	}

	/**
	 * 关于Topic通配符
	/：用来表示层次，比如a/b，a/b/c。
	#：表示匹配>=0个层次，比如a/#就匹配a/，a/b，a/b/c。
	单独的一个#表示匹配所有。
	不允许 a#和a/#/c。
	+：表示匹配一个层次，例如a/+匹配a/b，a/c，不匹配a/b/c。
	单独的一个+是允许的，a+不允许，a/+/b允许
	 * @param topic
	 * @param set
	 * @return
	 */
	private Set<String> findClientsByTopic(String topic,Set<String> set){
		char c,oc;
		int topLen = topic.length();
		boolean flg;
		for (Map.Entry<String/* topic */, ConcurrentHashMap<String/* clientId */, Subscription>> entry : subscriptionMap.entrySet()) {
			int seachLen = entry.getKey().length();
			if(topLen<seachLen)continue;
			flg = true;
			int j=0;
			for(int i=0;i<seachLen && flg;i++){
				c = entry.getKey().charAt(i);
				switch(c){
				case '+':
					do{
						oc = topic.charAt(j++);
					}while(oc!='/'&&j<topLen);
					if(j<topLen){
						if(i>=seachLen-1){
							flg=false;
							break;
						}
						j--;
					}else{
						set.addAll(entry.getValue().keySet());
						flg=false;
					}
					break;
				case '#':
					set.addAll(entry.getValue().keySet());
					flg=false;
					break;
				case '/':
					oc = topic.charAt(j++);
					if(c!=oc){
						if(i-j==2){
							set.addAll(entry.getValue().keySet());
							flg=false;
							break;
						}
						flg=false;
					}
					break;
				default:
					oc = topic.charAt(j++);
					if(c!=oc){
						flg=false;
						break;
					}
					break;
				}
			}
			if(flg && topLen==j)
				set.addAll(entry.getValue().keySet());
		}
		return set;
	}
	
	private PublishMessage ext2PublishMessage(PublishMessageExt m) {
		PublishMessage pm = new PublishMessage();
		pm.setDupFlag(m.isDupFlag());
		pm.setMessageID(m.getMessageID());
		pm.setMessageType(m.getMessageType());
		pm.setQos(m.getQos());
		pm.setRetainFlag(m.isRetainFlag());
		pm.setTopicName(m.getTopicName());
		pm.setPayload(ByteBuffer.wrap(m.getPayload()));
		return pm;
	}

	private void storeSub(Subscription sub) {
		ConcurrentHashMap<String/* clientId */, Subscription> subs = subscriptionMap.get(sub.getTopicFilter());
		if (subs == null) {
			ConcurrentHashMap<String/* clientId */, Subscription> newsubs = new ConcurrentHashMap();
			subs = subscriptionMap.putIfAbsent(sub.getTopicFilter(), newsubs);
			if (subs == null)
				subs = newsubs;
		}
		Subscription oldsub = subs.get(sub.getClientId());
		if (oldsub == null) {
			oldsub = subs.putIfAbsent(sub.getClientId(), sub);
		}
	}

	private void deleteSub(String clientId) {
		for (Map.Entry<String/* topic */, ConcurrentHashMap<String/* clientId */, Subscription>> entry : subscriptionMap.entrySet()) {
			entry.getValue().remove(clientId);
		}
	}

	private void deleteSub(String clientId, String topic) {
		ConcurrentHashMap<String/* clientId */, Subscription> subs = subscriptionMap.get(topic);
		if (subs != null) {
			subs.remove(clientId);
		}
	}

	private void sendPublish(String clientId, PublishMessage pubMessage) {
		LOG.debug("sendPublish invoked clientId <{}> on topic <{}> QoS {} retained {} messageID {}", clientId, pubMessage.getTopicName(),
				pubMessage.getQos(), false, pubMessage.getMessageID());

		if (pubMessage.getQos() != MessageType.QOS_MOST_ONE) {
			pubMessage.setMessageID(pubMessage.getMessageID());
		}

		ConnectionDescriptor cd = m_clientIDs.get(clientId);
		if (cd == null) {
			return;// throw new
					// RuntimeException(String.format("Can't find a ConnectionDescriptor for client <%s> in cache <%s>",
					// clientId, m_clientIDs));
		}
		disruptorPublish(new OutputMessagingEvent(cd.getSession(), pubMessage));
	}

	@MQTTMessage(message = ConnectMessage.class)
	void processConnect(ServerChannel session, ConnectMessage msg) {
		LOG.debug("processConnect for client {}", msg.getClientID());
		if (msg.getProcotolVersion() != VERSION_3_1 && msg.getProcotolVersion() != VERSION_3_1_1) {
			ConnAckMessage badProto = new ConnAckMessage();
			badProto.setReturnCode(ConnAckMessage.UNNACEPTABLE_PROTOCOL_VERSION);
			LOG.warn("processConnect sent bad proto ConnAck");
			session.write(badProto);
			session.close(false);
			return;
		}
		if (msg.getClientID() == null || msg.getClientID().length() > 23 || msg.getClientID().length() == 0) {
			ConnAckMessage okResp = new ConnAckMessage();
			okResp.setReturnCode(ConnAckMessage.IDENTIFIER_REJECTED);
			session.write(okResp);
			okResp = null;
			return;
		}
		// if an old client with the same ID already exists close its session.
		if (m_clientIDs.containsKey(msg.getClientID())) {
			// clean the subscriptions if the old used a cleanSession = true
			ServerChannel oldSession = m_clientIDs.get(msg.getClientID()).getSession();
			boolean cleanSession = (Boolean) oldSession.getAttribute(Constants.CLEAN_SESSION);
			if (cleanSession) {
				// cleanup topic subscriptions
				// cleanSession(msg.getClientID());
			}

			oldSession.close(false);
			oldSession = null;
		}

		ConnectionDescriptor connDescr = new ConnectionDescriptor(msg.getClientID(), session, msg.isCleanSession());
		m_clientIDs.put(msg.getClientID(), connDescr);

		int keepAlive = msg.getKeepAlive();
		LOG.debug("Connect with keepAlive {} s", keepAlive);
		session.setAttribute(Constants.KEEP_ALIVE, keepAlive);
		session.setAttribute(Constants.CLEAN_SESSION, msg.isCleanSession());
		// used to track the client in the subscription and publishing phases.
		session.setAttribute(Constants.ATTR_CLIENTID, msg.getClientID());

		session.setIdleTime(Math.round(keepAlive * 1.5f));

		// Handle will flag
		if (msg.isWillFlag()) {
			// byte willQos = msg.getWillQos();
			// byte[] willPayload = msg.getWillMessage().getBytes();
			// ByteBuffer bb = (ByteBuffer)
			// ByteBuffer.allocate(willPayload.length).put(willPayload).flip();
			// save the will testment in the clientID store
			// WillMessage will = new WillMessage(msg.getWillTopic(), bb,
			// msg.isWillRetain(), willQos);
			// m_willStore.put(msg.getClientID(), will);
			// will=null;
		}

		// handle user authentication
		if (msg.isUserFlag()) {
			String username = msg.getUsername();
			String pwd = msg.getPassword();
			if (msg.isPasswordFlag()) {
				Result<DataEntry> res = tairManager.get(1, username);
				if (res.getRc() == ResultCode.SUCCESS) {
					String oldpwd = (String) res.getValue().getValue();
					if (StringUtils.equals(pwd, oldpwd)) {
						connDescr.setAuthenticated(true);
					}
				}

			}

			if (!connDescr.isAuthenticated()) {
				ConnAckMessage okResp = new ConnAckMessage();
				okResp.setReturnCode(ConnAckMessage.BAD_USERNAME_OR_PASSWORD);
				session.write(okResp);
				return;
			}

		}

		if (msg.isCleanSession()) {
		}

		ConnAckMessage okResp = new ConnAckMessage();
		okResp.setReturnCode(ConnAckMessage.CONNECTION_ACCEPTED);
		LOG.debug("processConnect sent OK ConnAck");
		session.write(okResp);
		okResp = null;
		LOG.warn("Connected client ID <{}> with clean session {}", msg.getClientID(), msg.isCleanSession());

		LOG.info("Create persistent session for clientID {}", msg.getClientID());
		if (!msg.isCleanSession()) {
		}
	}

	@MQTTMessage(message = PubAckMessage.class)
	void processPubAck(ServerChannel session, PubAckMessage msg) {
		at_sub_count.incrementAndGet();
		String clientID = (String) session.getAttribute(Constants.ATTR_CLIENTID);
		int messageID = msg.getMessageID();
		LOG.info("Subscriber -- PUBACK --> SRV processPubAck invoked for clientID {} with messageID {}", clientID, messageID);

		// Remove the message from message store
		// m_messagesStore.cleanPersistedPublishMessage(clientID, messageID);
		String publishKey = String.format("%s%d", clientID, messageID);
		MQMessage body = new MQMessage(clientID, msg);
		try {
			Message mqmsg = new Message(CURRENT_TOPIC_UPG, body.getClientId(), publishKey, ObjectUtils.serialize(body));
			mqProducer.send(mqmsg, new SendCallback() {
				@Override
				public void onSuccess(SendResult sendResult) {
				}

				@Override
				public void onException(Throwable e) {	}
			});
		} catch (Exception e) {
			LOG.error("", e);
		}
		publishKey = null;
		clientID = null;
	}

	@MQTTMessage(message = PublishMessage.class)
	void processPublish(ServerChannel session, final PublishMessage msg) {
		at_pub_count.incrementAndGet();
		final String clientID = (String) session.getAttribute(Constants.ATTR_CLIENTID);
		final Integer messageID = msg.getMessageID();
		final String topic = msg.getTopicName();
		LOG.trace("PUB --PUBLISH--> SRV processPublish invoked for clientID {} with messageID {} on topic {}", clientID, messageID, topic);

		final byte qos = msg.getQos();
		ConnectionDescriptor cd = m_clientIDs.get(clientID);
		if (!cd.isAuthenticated() && qos > MessageType.QOS_MOST_ONE) {// QOS>0
																		// 需认证用户
			processConnectionLost(clientID);
		}
		final ByteBuffer message = msg.getPayload();
		final boolean retain = msg.isRetainFlag();
		String publishKey = String.format("%s%d", clientID, messageID);

		PublishMessageExt pmext = new PublishMessageExt();
		pmext.setPayload(message.array());
		pmext.setDupFlag(msg.isDupFlag());
		pmext.setMessageID(msg.getMessageID());
		pmext.setQos(msg.getQos());
		pmext.setRetainFlag(msg.isRetainFlag());
		pmext.setTopicName(msg.getTopicName());

		MQMessage body = new MQMessage(clientID, pmext);
		if (qos == MessageType.QOS_EXACTLY_ONCE) {
			// store the message in temp store
			byte[] bMsg = ObjectUtils.serialize(body);
			tairManager.put(1, publishKey, bMsg);
			processPublish(clientID, topic, qos, retain, messageID);
		} else {

			try {
				Message mqmsg = new Message(CURRENT_TOPIC_UPG, body.getClientId(), publishKey, ObjectUtils.serialize(body));
				if (qos == MessageType.QOS_MOST_ONE) {
					mqProducer.sendOneway(mqmsg);
					processPublish(clientID, topic, qos, retain, messageID);
				} else {
					mqProducer.send(mqmsg, new SendCallback() {
						@Override
						public void onSuccess(SendResult sendResult) {
							processPublish(clientID, topic, qos, retain, messageID);
						}
						@Override
						public void onException(Throwable e) {	}
					});
				}
			} catch (Exception e) {
				LOG.error("", e);
			}
		}

		body = null;
		publishKey = null;
	}

	private void processPublish(String clientID, String topic, byte qos, boolean retain, Integer messageID) {
		if (qos == MessageType.QOS_EXACTLY_ONCE) {
			// String publishKey = String.format("%s%d", clientID, messageID);
			PubRecMessage pubRecMessage = new PubRecMessage();
			pubRecMessage.setMessageID(messageID);
			disruptorPublish(new OutputMessagingEvent(m_clientIDs.get(clientID).getSession(), pubRecMessage));
		} else if (qos == MessageType.QOS_LEAST_ONE) {
			PubAckMessage pubAckMessage = new PubAckMessage();
			pubAckMessage.setMessageID(messageID);
			disruptorPublish(new OutputMessagingEvent(m_clientIDs.get(clientID).getSession(), pubAckMessage));
			LOG.debug("replying with PubAck to MSG ID {}", messageID);
		}

		if (retain) {
			if (qos == MessageType.QOS_MOST_ONE) {
				// QoS == 0 && retain => clean old retained
				// m_messagesStore.cleanRetained(topic);
			} else {
				// 存储至K/V
				// m_messagesStore.storeRetained(topic, message, qos);
			}
		}
	}

	@MQTTMessage(message = PubRelMessage.class)
	void processPubRel(ServerChannel session, PubRelMessage msg) {
		String clientID = (String) session.getAttribute(Constants.ATTR_CLIENTID);
		int messageID = msg.getMessageID();
		LOG.debug("PUB --PUBREL--> SRV processPubRel invoked for clientID {} ad messageID {}", clientID, messageID);
		String publishKey = String.format("%s%d", clientID, messageID);
		MQMessage body = new MQMessage(clientID, msg);
		try {
			Message mqmsg = new Message(CURRENT_TOPIC_UPG, body.getClientId(), publishKey, ObjectUtils.serialize(body));
			mqProducer.sendOneway(mqmsg);
		} catch (Exception e) {
			LOG.error("", e);
		}
	}

	@MQTTMessage(message = PubRecMessage.class)
	void processPubRec(ServerChannel session, final PubRecMessage msg) {
		final String clientID = (String) session.getAttribute(Constants.ATTR_CLIENTID);
		final int messageID = msg.getMessageID();
		String publishKey = String.format("%s%d", clientID, messageID);

		MQMessage body = new MQMessage(clientID, msg);
		try {
			Message mqmsg = new Message(CURRENT_TOPIC_UPG, body.getClientId(), publishKey, ObjectUtils.serialize(body));
			mqProducer.send(mqmsg, new SendCallback() {
				@Override
				public void onSuccess(SendResult sendResult) {
					PubRelMessage pubRelMessage = new PubRelMessage();
					pubRelMessage.setMessageID(messageID);
					pubRelMessage.setQos(MessageType.QOS_LEAST_ONE);
					// m_clientIDs.get(clientID).getSession().write(pubRelMessage);
					disruptorPublish(new OutputMessagingEvent(m_clientIDs.get(clientID).getSession(), pubRelMessage));
				}
				@Override
				public void onException(Throwable e) {}
			});
		} catch (Exception e) {
			LOG.error("", e);
		}

		// once received a PUBREC reply with a PUBREL(messageID)
		LOG.debug("SRV <--PUBREC-- SUB processPubRec invoked for clientID {} ad messageID {}", clientID, messageID);

	}

	@MQTTMessage(message = PubCompMessage.class)
	void processPubComp(ServerChannel session, PubCompMessage msg) {
		String clientID = (String) session.getAttribute(Constants.ATTR_CLIENTID);
		int messageID = msg.getMessageID();
		LOG.debug("SRV <--PUBCOMP-- SUB processPubComp invoked for clientID {} ad messageID {}", clientID, messageID);
		// once received the PUBCOMP then remove the message from the temp
		// memory
		String publishKey = String.format("%s%d", clientID, messageID);
		// m_messagesStore.cleanInFlight(publishKey);
		MQMessage body = new MQMessage(clientID, msg);
		try {
			Message mqmsg = new Message(CURRENT_TOPIC_UPG, body.getClientId(), publishKey, ObjectUtils.serialize(body));
			mqProducer.sendOneway(mqmsg);
		} catch (Exception e) {
			LOG.error("", e);
		}
	}

	@MQTTMessage(message = DisconnectMessage.class)
	void processDisconnect(ServerChannel session, DisconnectMessage msg) throws InterruptedException {
		String clientID = (String) session.getAttribute(Constants.ATTR_CLIENTID);
		boolean cleanSession = (Boolean) session.getAttribute(Constants.CLEAN_SESSION);
		if (cleanSession) {
			// cleanup topic subscriptions
			// cleanSession(clientID);
		}
		// m_notifier.disconnect(evt.getSession());
		m_clientIDs.remove(clientID);
		session.close(true);
		deleteSub(clientID);
		// de-activate the subscriptions for this ClientID
		// subscriptions.deactivate(clientID);
		// cleanup the will store
		// m_willStore.remove(clientID);
		LOG.warn("Disconnected client <{}> with clean session {}", clientID, cleanSession);
	}

	@MQTTMessage(message = UnsubscribeMessage.class)
	void processUnsubscribe(ServerChannel session, UnsubscribeMessage msg) {
		List<String> topics = msg.topicFilters();
		int messageID = msg.getMessageID();
		String clientID = (String) session.getAttribute(Constants.ATTR_CLIENTID);
		LOG.debug("SUB --UNSUBSCRIBE--> SRV processUnsubscribe invoked, removing subscription on topics {}, for clientID <{}>", topics, clientID);
		for (String topic : topics) {
			deleteSub(clientID, topic);

			/*
			 * try { mqConsumer.unsubscribe(topic); } catch (Exception e) {
			 * LOG.error("",e); m_clientIDs.remove(clientID);
			 * session.close(true); deleteSub(clientID); return; }
			 */

		}
		// ack the client
		UnsubAckMessage ackMessage = new UnsubAckMessage();
		ackMessage.setMessageID(messageID);

		LOG.info("replying with UnsubAck to MSG ID {}", messageID);
		session.write(ackMessage);

	}

	@MQTTMessage(message = SubscribeMessage.class)
	void processSubscribe(ServerChannel session, SubscribeMessage msg) {
		String clientID = (String) session.getAttribute(Constants.ATTR_CLIENTID);
		Boolean cleanSession = null;
		try {
			cleanSession = (Boolean) session.getAttribute(Constants.CLEAN_SESSION);
		} catch (NullPointerException e) {
			e.printStackTrace();
		}
		LOG.debug("SUB --SUBSCRIBE--> SRV processSubscribe invoked for clientID {} ad messageID {}", clientID, msg.getMessageID());
		// ack the client
		SubAckMessage ackMessage = new SubAckMessage();
		ackMessage.setMessageID(msg.getMessageID());
		long maxOffset = -1;
		String topic;
		ConnectionDescriptor cd = m_clientIDs.get(clientID);
		if(cd==null){
			processConnectionLost(clientID);
			return;
		}
		for (SubscribeMessage.Couple req : msg.subscriptions()) {
			byte qos = req.getQos();
			ackMessage.addType(qos);
			topic = req.getTopicFilter();
			LOG.debug("\tTOPIC: {}", topic);
			int start = StringUtils.indexOf(topic, "?");
			if (start > 0) {
				topic = StringUtils.substring(req.getTopicFilter(), 0, start);
				String strArgs = StringUtils.substring(req.getTopicFilter(), start + 1);
				String args[] = StringUtils.split(strArgs, "=");
				maxOffset = Long.valueOf(args[1]);
			}

			if (StringUtils.startsWith(topic, Constants.TOPIC_PERSONAL_PREFIX)) {
				if (!cd.isAuthenticated()) {// 订阅P2P消息需认证用户
					processConnectionLost(clientID);
				}
				continue;// topic=Constants.TOPIC_NULL;
			}
			if (!cd.isAuthenticated() && qos > MessageType.QOS_MOST_ONE) {// QOS>0
																			// 需认证用户
				processConnectionLost(clientID);
			}
			Subscription newSubscription = new Subscription(clientID, topic, qos, cleanSession, maxOffset);
			// subscribeSingleTopic(newSubscription, req.getTopicFilter());
			storeSub(newSubscription);

			/*
			 * if(StringUtils.startsWith(req.getTopicFilter(),
			 * Constants.TOPIC_NULL))continue;
			 * if(StringUtils.startsWith(req.getTopicFilter(),
			 * Constants.TOPIC_PERSISTER_GROUP_PREFIX))continue;
			 * 
			 * try { mqConsumer.subscribe(req.getTopicFilter(), "*"); } catch
			 * (MQClientException e) { LOG.error("",e);
			 * m_clientIDs.remove(clientID); session.close(true);
			 * deleteSub(clientID); return; }
			 */
		}

		LOG.debug("replying with SubAck to MSG ID {}", msg.getMessageID());
		session.write(ackMessage);
		ackMessage = null;
	}

	void processConnectionLost(String clientID) {

		// If already removed a disconnect message was already processed for
		// this clientID
		if (m_clientIDs.remove(clientID) != null) {
			deleteSub(clientID);
			// de-activate the subscriptions for this ClientID
			// subscriptions.deactivate(clientID);
			ConnectionDescriptor cd = m_clientIDs.remove(clientID);
			if (cd != null) {
				ServerChannel s = cd.getSession();
				if (s != null)
					s.close(true);
			}
			LOG.info("Lost connection with client <{}>", clientID);
		}
		// publish the Will message (if any) for the clientID
		/*
		 * if (m_willStore.containsKey(clientID)) { WillMessage will =
		 * m_willStore.get(clientID); processPublish(will, clientID);
		 * m_willStore.remove(clientID); }
		 */
	}

	private void disruptorPublish(OutputMessagingEvent msgEvent) {
		LOG.debug("disruptorPublish publishing event on output {}", msgEvent);
		long sequence = m_ringBuffer.next();
		try {
			ValueEvent event = m_ringBuffer.get(sequence);
			event.setEvent(msgEvent);
		} finally {
			m_ringBuffer.publish(sequence);
		}

	}

	@Override
	public void onEvent(ValueEvent tevn, long sequence, boolean endOfBatch) throws Exception {
		MessagingEvent evt = tevn.getEvent();
		// It's always of type OutputMessagingEvent
		OutputMessagingEvent outEvent = (OutputMessagingEvent) evt;
		long time = System.currentTimeMillis();
		try {
			outEvent.getChannel().write(outEvent.getMessage());
		} catch (Throwable t) {
			LOG.error("{}", System.currentTimeMillis() - time, t);
		}
		outEvent = null;
	}

}
