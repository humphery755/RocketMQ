package com.alibaba.rocketmq.example.tmp;

import com.alibaba.rocketmq.common.protocol.ResponseCode;
import com.alibaba.rocketmq.common.protocol.header.*;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.filter.FilterAPI;
import com.alibaba.rocketmq.common.message.MessageDecoder;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.protocol.RequestCode;
import com.alibaba.rocketmq.common.protocol.heartbeat.ConsumeType;
import com.alibaba.rocketmq.common.protocol.heartbeat.ConsumerData;
import com.alibaba.rocketmq.common.protocol.heartbeat.HeartbeatData;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import com.alibaba.rocketmq.common.protocol.heartbeat.ProducerData;
import com.alibaba.rocketmq.common.protocol.heartbeat.SubscriptionData;
import com.alibaba.rocketmq.common.sysflag.PullSysFlag;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;

public class ConsumerRocketClient extends Client {

	@Override
	SimpleChannelInboundHandler<RemotingCommand> getHandler() {
		return new EchoClientHandler();
	}

	public void start() {

	}

	public static void main(String[] args) throws Exception {

		ConsumerRocketClient c = new ConsumerRocketClient();
		new TmpNameSrvClient(c, "127.0.0.1", 9876).start();
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
}

@Sharable
class EchoClientHandler extends SimpleChannelInboundHandler<RemotingCommand> {
	Map<Integer, Response> responseTable = new ConcurrentHashMap();
	ClientRemotingProcessor crp = new ClientRemotingProcessor(null);
	/**
	 * 是否每次拉消息时，都上传订阅关系
	 */
	boolean postSubscriptionWhenPull = false;
	// 订阅关系，用户配置的原始数据
	protected final static ConcurrentHashMap<String /* topic */, SubscriptionData> subscriptionInner = new ConcurrentHashMap<String, SubscriptionData>();
	static String CONSUMER_GROUP = "please_rename_unique_group_name_3";
	
	static {
		try {
			
			SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(CONSUMER_GROUP, "TopicTest1", "TagA || TagC || TagD");
			subscriptionInner.put("TopicTest1", subscriptionData);

			subscriptionData = FilterAPI.buildSubscriptionData(CONSUMER_GROUP, "%RETRY%" + CONSUMER_GROUP, SubscriptionData.SUB_ALL);
			
			subscriptionInner.put("%RETRY%"+CONSUMER_GROUP, subscriptionData);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 此方法会在连接到server后被调用
	 * */
	public void channelActive(ChannelHandlerContext ctx) {
		/*
		 * byte[] body="hello world!".getBytes(); int bodyLength=body.length; //
		 * 1> header length size int length = 4;
		 * 
		 * // 2> header data length byte[] headerData =
		 * "{\"code\":101,\"extFields\":{\"key\":\"192.168.88.54\",\"namespace\":\"PROJECT_CONFIG\"},\"flag\":0,\"language\":\"JAVA\",\"opaque\":0,\"version\":74}"
		 * .getBytes(); length += headerData.length;
		 * 
		 * // 3> body data length length += bodyLength;
		 * 
		 * ByteBuffer header = ByteBuffer.allocate(4 + length - bodyLength);
		 * 
		 * // length header.putInt(length);
		 * 
		 * // header length header.putInt(headerData.length);
		 * 
		 * // header data header.put(headerData);
		 * 
		 * header.flip();
		 * 
		 * //return result;
		 * 
		 * ctx.write(header); //byte[] body = remotingCommand.getBody(); if
		 * (body != null) { ctx.write(body); }
		 */
		/*
		 * GetRouteInfoRequestHeader requestHeader = new
		 * GetRouteInfoRequestHeader(); requestHeader.setTopic("TopicTest1");
		 * 
		 * RemotingCommand request = RemotingCommand.createRequestCommand(
		 * RequestCode.GET_ROUTEINTO_BY_TOPIC, requestHeader);
		 * ctx.writeAndFlush(request);
		 */
		// ctx.write(Unpooled.copiedBuffer("Netty rocks!", CharsetUtil.UTF_8));
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
		heartbeatData.setClientID("192.168.88.54@" + ConsumerRocketClient.getPid());
		ConsumerData consumerData = new ConsumerData();
		consumerData.setGroupName(CONSUMER_GROUP);
		consumerData.setConsumeType(ConsumeType.CONSUME_PASSIVELY);
		consumerData.setMessageModel(MessageModel.CLUSTERING);
		consumerData.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
		consumerData.setUnitMode(false);

		consumerData.getSubscriptionDataSet().add(subscriptionInner.get("TopicTest1"));

		consumerData.getSubscriptionDataSet().add(subscriptionInner.get("%RETRY%"+CONSUMER_GROUP));

		heartbeatData.getConsumerDataSet().add(consumerData);

		ProducerData pd = new ProducerData();
		pd.setGroupName("CLIENT_INNER_PRODUCER");
		heartbeatData.getProducerDataSet().add(pd);
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.HEART_BEAT, null);
		request.setBody(heartbeatData.encode());

		final Response responseFuture = new Response() {
			@Override
			public void excute(RemotingCommand cmd) {
				System.out.println("BClient received: HEART_BEAT " + cmd);
				getConsumerIdList(ctx, "please_rename_unique_group_name_3");
			}

		};

		responseTable.put(request.getOpaque(), responseFuture);
		ctx.writeAndFlush(request);
	}

	public void getConsumerIdList(final ChannelHandlerContext ctx, final String consumerGroup) {
		GetConsumerListByGroupRequestHeader requestHeader = new GetConsumerListByGroupRequestHeader();
		requestHeader.setConsumerGroup(consumerGroup);

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_LIST_BY_GROUP, requestHeader);
		final Response responseFuture = new Response() {
			@Override
			public void excute(RemotingCommand cmd) {
				byte[] body = cmd.getBody();
				if (body != null) {
					GetConsumerListByGroupResponseBody res = GetConsumerListByGroupResponseBody
							.decode(body, GetConsumerListByGroupResponseBody.class);

					System.out.println("BClient received: GET_CONSUMER_LIST_BY_GROUP " + cmd);
					queryConsumerOffset(ctx, consumerGroup, 0);
/*					queryConsumerOffset(ctx, consumerGroup, 1);
					queryConsumerOffset(ctx, consumerGroup, 2);
					queryConsumerOffset(ctx, consumerGroup, 3);
					queryConsumerOffset(ctx, consumerGroup, 4);
					queryConsumerOffset(ctx, consumerGroup, 5);
					queryConsumerOffset(ctx, consumerGroup, 6);
					queryConsumerOffset(ctx, consumerGroup, 7);*/
					// queryConsumerOffset(ctx,"%RETRY%"+consumerGroup);
				}
			}

		};
		responseTable.put(request.getOpaque(), responseFuture);
		ctx.writeAndFlush(request);
	}

	public void queryConsumerOffset(final ChannelHandlerContext ctx, final String consumerGroup, final Integer queueId) {
		QueryConsumerOffsetRequestHeader requestHeader = new QueryConsumerOffsetRequestHeader();
		requestHeader.setTopic("TopicTest1");
		requestHeader.setConsumerGroup(consumerGroup);
		requestHeader.setQueueId(queueId);// 0,1,2,3,4,5,6,7
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_CONSUMER_OFFSET, requestHeader);
		final Response responseFuture = new Response() {
			@Override
			public void excute(RemotingCommand cmd) {
				byte[] body = cmd.getBody();
				if (body != null) {

				}
				System.out.println("BClient received: QUERY_CONSUMER_OFFSET " + cmd);
				// if (queueId == 1)
				PULL_MESSAGE(ctx, consumerGroup, queueId, Long.valueOf(cmd.getExtFields().get("offset")));
			}

		};
		responseTable.put(request.getOpaque(), responseFuture);
		ctx.writeAndFlush(request);
	}

	public void updateConsumerOffset(final ChannelHandlerContext ctx, final String consumerGroup, final Integer queueId, final Long offset) {
		UpdateConsumerOffsetRequestHeader requestHeader = new UpdateConsumerOffsetRequestHeader();
		requestHeader.setTopic("TopicTest1");
		requestHeader.setConsumerGroup(consumerGroup);
		requestHeader.setQueueId(queueId);
		requestHeader.setCommitOffset(offset);

		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_CONSUMER_OFFSET, requestHeader);
		request.setFlag(2);
		final Response responseFuture = new Response() {
			@Override
			public void excute(RemotingCommand cmd) {
				byte[] body = cmd.getBody();
				if (body != null) {

				}
				System.out.println("BClient received: UPDATE_CONSUMER_OFFSET " + cmd);
			}

		};
		responseTable.put(request.getOpaque(), responseFuture);
		ctx.writeAndFlush(request);
	}

    private void back(final ChannelHandlerContext ctx, final String consumerGroup, final Integer queueId, final String msgId, final Long offset){
        ConsumerSendMsgBackRequestHeader requestHeader = new ConsumerSendMsgBackRequestHeader();


        requestHeader.setGroup(consumerGroup);
        requestHeader.setOriginTopic("TopicTest1");
        requestHeader.setOffset(offset);//msg.getCommitLogOffset());
        //int delayLevel = ctx.getDelayLevelWhenNextConsume();
        //requestHeader.setDelayLevel(delayLevel);
        requestHeader.setOriginMsgId(msgId);

        RemotingCommand request =
                RemotingCommand.createRequestCommand(RequestCode.CONSUMER_SEND_MSG_BACK, requestHeader);
        request.setFlag(2);
        final Response responseFuture = new Response() {
            @Override
            public void excute(RemotingCommand cmd) {
                byte[] body = cmd.getBody();
                if (body != null) {

                }
                System.out.println("BClient received: CONSUMER_SEND_MSG_BACK " + cmd);
            }

        };
        responseTable.put(request.getOpaque(), responseFuture);
        ctx.writeAndFlush(request);
    }

	public void PULL_MESSAGE(final ChannelHandlerContext ctx, final String consumerGroup, final Integer queueId, final Long offset) {
		PullMessageRequestHeader requestHeader = new PullMessageRequestHeader();
		requestHeader.setTopic("TopicTest1");
		requestHeader.setConsumerGroup(consumerGroup);
		requestHeader.setQueueId(queueId);// 0,1,2,3,4,5,6,7

		SubscriptionData sd = subscriptionInner.get("TopicTest1");
		boolean commitOffsetEnable = false;
		String subExpression = null;
		boolean classFilter = false;
		// if (MessageModel.CLUSTERING ==
		// this.defaultMQPushConsumer.getMessageModel()) {
		/*
		 * commitOffsetValue =
		 * this.offsetStore.readOffset(pullRequest.getMessageQueue(),
		 * ReadOffsetType.READ_FROM_MEMORY);
		 */
		if (offset > 0) {
			commitOffsetEnable = true;
		}
		// }

		if (postSubscriptionWhenPull && /*
										 * this.defaultMQPushConsumer.
										 * isPostSubscriptionWhenPull() &&
										 */!sd.isClassFilterMode()) {
			subExpression = sd.getSubString();
		}

		classFilter = sd.isClassFilterMode();

		int sysFlag = PullSysFlag.buildSysFlag(//
				commitOffsetEnable, // commitOffset
				true, // suspend
				subExpression != null,// subscription
				classFilter // class filter
				);

		// int sysFlag =
		// MessageSysFlag.MultiTagsFlag|MessageSysFlag.CompressedFlag;//PullSysFlag.buildSysFlag(false,
		// false, true, false);

		requestHeader.setSysFlag(sysFlag);
		requestHeader.setCommitOffset(offset);
		requestHeader.setQueueOffset(offset);

		requestHeader.setSuspendTimeoutMillis(15000L);
		requestHeader.setSubVersion(15000L);//System.currentTimeMillis() - 15000);
		requestHeader.setMaxMsgNums(32);
		RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, requestHeader);
		final Response responseFuture = new Response() {
			@Override
			public void excute(RemotingCommand cmd) {
				byte[] body = cmd.getBody();
				if (body != null && cmd.getCode() == 0) {
					MessageExt me = MessageDecoder.decode(ByteBuffer.wrap(body));
					System.err.println("<< " + me);
					// if ( failure ) {
                    // Consumer将处理不了的消息发回服务器
                    back(ctx, consumerGroup, me.getQueueId(), me.getMsgId(), me.getCommitLogOffset());
					// }

                    updateConsumerOffset(ctx, consumerGroup, me.getQueueId(), me.getCommitLogOffset());
				} else
					System.out.println("BClient received: PULL_MESSAGE " + cmd);

			}

		};
		responseTable.put(request.getOpaque(), responseFuture);
		ctx.writeAndFlush(request);
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