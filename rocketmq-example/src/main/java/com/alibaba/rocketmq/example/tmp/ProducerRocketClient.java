package com.alibaba.rocketmq.example.tmp;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.alibaba.rocketmq.client.VirtualEnvUtil;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.producer.LocalTransactionState;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.client.producer.SendStatus;
import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.filter.FilterAPI;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageConst;
import com.alibaba.rocketmq.common.message.MessageDecoder;
import com.alibaba.rocketmq.common.message.MessageId;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.RequestCode;
import com.alibaba.rocketmq.common.protocol.ResponseCode;
import com.alibaba.rocketmq.common.protocol.header.EndTransactionRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.SendMessageRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.SendMessageRequestHeaderV2;
import com.alibaba.rocketmq.common.protocol.header.SendMessageResponseHeader;
import com.alibaba.rocketmq.common.protocol.heartbeat.HeartbeatData;
import com.alibaba.rocketmq.common.protocol.heartbeat.ProducerData;
import com.alibaba.rocketmq.common.protocol.heartbeat.SubscriptionData;
import com.alibaba.rocketmq.common.sysflag.MessageSysFlag;
import com.alibaba.rocketmq.remoting.common.RemotingUtil;
import com.alibaba.rocketmq.remoting.exception.RemotingCommandException;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;

public class ProducerRocketClient extends Client {

	public void start() {

	}

	public static void main(String[] args) throws Exception {
		ProducerRocketClient prc = new ProducerRocketClient();
		new TmpNameSrvClient(prc, "127.0.0.1", 9876).start();
		// Channel c = new ProducerRocketClient().getChannel("127.0.0.1:10911");
	}

	public static int getPid() {
		RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
		String name = runtime.getName(); // format: "pid@hostname"
		try {
			return Integer.parseInt(name.substring(0, name.indexOf('@')));
		} catch (Exception e) {
			return -1;
		}
	}

	@Override
	SimpleChannelInboundHandler<RemotingCommand> getHandler() {
		// TODO Auto-generated method stub
		return new ProducerClientHandler();
	}
}

@Sharable
class ProducerClientHandler extends SimpleChannelInboundHandler<RemotingCommand> {
	Map<Integer, Response> responseTable = new ConcurrentHashMap();
	ClientRemotingProcessor crp = new ClientRemotingProcessor(null);
	// 虚拟运行环境相关的project group
	private String projectGroupPrefix;
	/**
	 * 是否每次拉消息时，都上传订阅关系
	 */
	boolean postSubscriptionWhenPull = false;
	/**
	 * 是否为单元化的发布者
	 */
	private boolean unitMode = false;
	// 订阅关系，用户配置的原始数据
	protected final static ConcurrentHashMap<String /* topic */, SubscriptionData> subscriptionInner = new ConcurrentHashMap<String, SubscriptionData>();

	static {
		try {
			SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData("please_rename_unique_group_name_3", "TopicTest1",
					"TagA || TagC || TagD");
			subscriptionInner.put("TopicTest1", subscriptionData);

			subscriptionData = FilterAPI.buildSubscriptionData("please_rename_unique_group_name_3", "%RETRY%please_rename_unique_group_name_3", "*");
			subscriptionInner.put("%RETRY%please_rename_unique_group_name_3", subscriptionData);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 此方法会在连接到server后被调用
	 * */
	public void channelActive(ChannelHandlerContext ctx) {
		sendHeartbeat(ctx);
	}

	/**
	 * 此方法会在接收到server数据后调用
	 * */
	public void channelRead0(ChannelHandlerContext ctx, RemotingCommand cmd) {
		/*
		 * System.out.println("Client received: " +
		 * ByteBufUtil.hexDump(in.readBytes(in.readableBytes())));
		 */
		if (cmd != null) {
			switch (cmd.getType()) {
			case REQUEST_COMMAND:
				processRequest(ctx, cmd);
				System.out.println("BClient received: REQUEST_COMMAND " + cmd);
				break;
			case RESPONSE_COMMAND:
				// processResponseCommand(ctx, cmd);

				Response responseFuture = responseTable.get(cmd.getOpaque());

				if (responseFuture != null) {
					responseFuture.excute(cmd);
				}

				break;
			default:
				System.out.println("BClient received: default " + cmd);
				break;
			}

		}
	}

	private void sendHeartbeat(final ChannelHandlerContext ctx) {
		HeartbeatData heartbeatData = new HeartbeatData();
		heartbeatData.setClientID("192.168.88.54@" + ProducerRocketClient.getPid());
		ProducerData producerData = new ProducerData();
		producerData.setGroupName("please_rename_unique_group_name_3");

		heartbeatData.getProducerDataSet().add(producerData);

		ProducerData pd = new ProducerData();
		pd.setGroupName("CLIENT_INNER_PRODUCER");
		heartbeatData.getProducerDataSet().add(pd);
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.HEART_BEAT, null);
		request.setBody(heartbeatData.encode());

		final Response responseFuture = new Response() {
			@Override
			public void excute(RemotingCommand cmd) {
				System.out.println("BClient received: HEART_BEAT " + cmd);
				sendMessageInTransaction(ctx, "please_rename_unique_group_name_3");
			}

		};

		responseTable.put(request.getOpaque(), responseFuture);
		ctx.writeAndFlush(request);
	}

	public void sendMessageInTransaction(final ChannelHandlerContext ctx, final String providerGroup) {
		SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();
		requestHeader.setProducerGroup(providerGroup);
		requestHeader.setBornTimestamp(System.currentTimeMillis());
		Map properties = getProperties();
		properties.put(MessageConst.PROPERTY_TRANSACTION_PREPARED, "true");
		properties.put(MessageConst.PROPERTY_KEYS, "KEY----------------1");
		properties.put(MessageConst.PROPERTY_PRODUCER_GROUP,providerGroup);
		requestHeader.setProperties(MessageDecoder.messageProperties2String(properties));
		requestHeader.setQueueId(1);
		requestHeader.setReconsumeTimes(0);
		requestHeader.setTopic("TopicTest1");
		requestHeader.setUnitMode(unitMode);
		requestHeader.setFlag(0);
		requestHeader.setSysFlag(0);
		requestHeader.setQueueId(1);
		requestHeader.setDefaultTopic("TBW102");
		requestHeader.setDefaultTopicQueueNums(4);

		SendMessageRequestHeaderV2 requestHeaderV2 = SendMessageRequestHeaderV2.createSendMessageRequestHeaderV2(requestHeader);
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE_V2, requestHeaderV2);
		request.setBody("Hello World!".getBytes());

		final Response responseFuture = new Response() {
			@Override
			public void excute(RemotingCommand cmd) {
				SendMessageResponseHeader res = null;

				try {
					res = processSendResponse(cmd);
					endTransaction(ctx,providerGroup,res);
				} catch (Exception e) {
					e.printStackTrace();
				}
				// queryConsumerOffset(ctx,"%RETRY%"+consumerGroup);
			}

		};
		responseTable.put(request.getOpaque(), responseFuture);
		ctx.writeAndFlush(request);
	}

	private void endTransaction(final ChannelHandlerContext ctx, final String providerGroup,SendMessageResponseHeader sendResult)  {
		MessageId id=null;
		try {
			id = MessageDecoder.decodeMessageId(sendResult.getMsgId());
		} catch (UnknownHostException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			return ;
		}
		final String addr = RemotingUtil.socketAddress2String(id.getAddress());
		EndTransactionRequestHeader requestHeader = new EndTransactionRequestHeader();
		requestHeader.setCommitLogOffset(id.getOffset());
		//switch (localTransactionState) {
		
		//case ROLLBACK_MESSAGE:
			requestHeader.setCommitOrRollback(MessageSysFlag.TransactionRollbackType);
			//break;
		//case UNKNOW:
			requestHeader.setCommitOrRollback(MessageSysFlag.TransactionNotType);
			//break;
		//case COMMIT_MESSAGE:
			requestHeader.setCommitOrRollback(MessageSysFlag.TransactionCommitType);
			//break;
		//default:
			//break;
		//}

		requestHeader.setProducerGroup(providerGroup);
		requestHeader.setTranStateTableOffset(sendResult.getQueueOffset());
		requestHeader.setMsgId(sendResult.getMsgId());
		String remark = null;//localException != null ? ("executeLocalTransactionBranch exception: " + localException.toString()) : null;
		
		 // 添加虚拟运行环境相关的projectGroupPrefix
        if (!UtilAll.isBlank(projectGroupPrefix)) {
            requestHeader.setProducerGroup(VirtualEnvUtil.buildWithProjectGroup(
                requestHeader.getProducerGroup(), projectGroupPrefix));
        }

        RemotingCommand request =
                RemotingCommand.createRequestCommand(RequestCode.END_TRANSACTION, requestHeader);

        request.setRemark(remark);
        final Response responseFuture = new Response() {
			@Override
			public void excute(RemotingCommand cmd) {
				byte body[] = cmd.getBody();
			}

		};
		responseTable.put(request.getOpaque(), responseFuture);
		ctx.writeAndFlush(request);
	}

	private Map<String, String> getProperties() {
		Map m = new HashMap();
		m.put(MessageConst.PROPERTY_TAGS, "TagA");
		m.put(MessageConst.PROPERTY_WAIT_STORE_MSG_OK, "true");
		return m;
	}

	private SendMessageResponseHeader processSendResponse(//
			/*
			 * final String brokerName,// final Message msg,//
			 */
			final RemotingCommand response//
	) throws MQBrokerException, RemotingCommandException {
		switch (response.getCode()) {
		case ResponseCode.FLUSH_DISK_TIMEOUT:
		case ResponseCode.FLUSH_SLAVE_TIMEOUT:
		case ResponseCode.SLAVE_NOT_AVAILABLE: {
			// TODO LOG
		}
		case ResponseCode.SUCCESS: {
			SendStatus sendStatus = SendStatus.SEND_OK;
			switch (response.getCode()) {
			case ResponseCode.FLUSH_DISK_TIMEOUT:
				sendStatus = SendStatus.FLUSH_DISK_TIMEOUT;
				break;
			case ResponseCode.FLUSH_SLAVE_TIMEOUT:
				sendStatus = SendStatus.FLUSH_SLAVE_TIMEOUT;
				break;
			case ResponseCode.SLAVE_NOT_AVAILABLE:
				sendStatus = SendStatus.SLAVE_NOT_AVAILABLE;
				break;
			case ResponseCode.SUCCESS:
				sendStatus = SendStatus.SEND_OK;
				break;
			default:
				assert false;
				break;
			}

			SendMessageResponseHeader responseHeader = (SendMessageResponseHeader) response
					.decodeCommandCustomHeader(SendMessageResponseHeader.class);

			/*
			 * MessageQueue messageQueue = new MessageQueue(msg.getTopic(),
			 * brokerName, responseHeader.getQueueId());
			 */

			return responseHeader;
			/*
			 * return new SendResult(sendStatus, responseHeader.getMsgId(),
			 * messageQueue, responseHeader.getQueueOffset(),
			 * projectGroupPrefix);
			 */
		}
		default:
			break;
		}

		throw new MQBrokerException(response.getCode(), response.getRemark());
	}

	public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) {
		try {
			switch (request.getCode()) {
			case RequestCode.CHECK_TRANSACTION_STATE:
				return ClientRemotingProcessor.checkTransactionState(ctx, request);
			case RequestCode.NOTIFY_CONSUMER_IDS_CHANGED:
				return ClientRemotingProcessor.notifyConsumerIdsChanged(ctx, request);
			case RequestCode.RESET_CONSUMER_CLIENT_OFFSET:
				return ClientRemotingProcessor.resetOffset(ctx, request);
			case RequestCode.GET_CONSUMER_STATUS_FROM_CLIENT:
				return ClientRemotingProcessor.getConsumeStatus(ctx, request);

			case RequestCode.GET_CONSUMER_RUNNING_INFO:
				return ClientRemotingProcessor.getConsumerRunningInfo(ctx, request);

			case RequestCode.CONSUME_MESSAGE_DIRECTLY:
				return ClientRemotingProcessor.consumeMessageDirectly(ctx, request);
			default:
				System.out.println("BClient received: processRequest " + request);
				break;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 捕捉到异常
	 * */
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
		cause.printStackTrace();
		ctx.close();
	}

}