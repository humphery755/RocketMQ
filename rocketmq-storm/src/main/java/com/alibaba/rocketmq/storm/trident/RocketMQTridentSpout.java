package com.alibaba.rocketmq.storm.trident;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.consumer.PullStatus;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import com.alibaba.rocketmq.remoting.exception.RemotingTimeoutException;
import com.alibaba.rocketmq.storm.MessageConsumerManager;
import com.alibaba.rocketmq.storm.domain.BatchMessage;
import com.alibaba.rocketmq.storm.domain.RocketMQConfig;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;

import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IPartitionedTridentSpout;
import storm.trident.spout.ISpoutPartition;
import storm.trident.topology.TransactionAttempt;

/**
 * @author Von Gosling
 */
public class RocketMQTridentSpout implements IPartitionedTridentSpout<List<MessageQueue>, ISpoutPartition, BatchMessage> {

	private static final long serialVersionUID = 8972193358178718167L;

	private static final Logger LOG = LoggerFactory.getLogger(RocketMQTridentSpout.class);

	private static final ConcurrentMap<String, List<MessageQueue>> cachedMessageQueue = new MapMaker().makeMap();
	private RocketMQConfig config;
	private static DefaultMQPullConsumer consumer;

	public RocketMQTridentSpout() {
	}

	public RocketMQTridentSpout(RocketMQConfig config) throws MQClientException {
		this.config = config;
		consumer = (DefaultMQPullConsumer) MessageConsumerManager.getConsumerInstance(config, null, false);
		consumer.setInstanceName(UUID.randomUUID().toString());
		consumer.start();
	}

	private List<MessageQueue> getMessageQueue(String topic) throws MQClientException {
		List<MessageQueue> cachedQueue = Lists.newArrayList();
		cachedQueue = cachedMessageQueue.get(config.getTopic());
		if (cachedQueue == null) {
			Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues(config.getTopic());
			cachedQueue = Lists.newArrayList(mqs);
			cachedMessageQueue.put(config.getTopic(), cachedQueue);

		}
		return cachedQueue;
	}

	class RocketMQCoordinator implements Coordinator<List<MessageQueue>> {

		@Override
		public List<MessageQueue> getPartitionsForBatch() {
			List<MessageQueue> queue = Lists.newArrayList();
			try {
				queue = getMessageQueue(config.getTopic());
			} catch (Exception e) {
				e.printStackTrace();
			}
			return queue;
		}

		@Override
		public boolean isReady(long txid) {
			return true;
		}

		@Override
		public void close() {
			LOG.info("close coordinator!");
		}

	}

	class RocketMQEmitter implements Emitter<List<MessageQueue>, ISpoutPartition, BatchMessage> {

		@Override
		public List<ISpoutPartition> getOrderedPartitions(List<MessageQueue> allPartitionInfo) {
			List<ISpoutPartition> partition = Lists.newArrayList();
			for (final MessageQueue queue : allPartitionInfo) {
				partition.add(new ISpoutPartition() {
					@Override
					public String getId() {
						return String.valueOf(queue.getQueueId());
					}
				});
			}
			return partition;
		}

		@Override
		public BatchMessage emitPartitionBatchNew(TransactionAttempt tx, TridentCollector collector, ISpoutPartition partition, BatchMessage lastPartitionMeta) {
			long index = 0;
			BatchMessage batchMessages = null;
			MessageQueue mq = null;
			try {
				mq = getMessageQueue(config.getTopic()).get(Integer.parseInt(partition.getId()));
				/*if (lastPartitionMeta == null) {
					index = consumer.fetchConsumeOffset(mq, true);
					index = index == -1 ? 0 : index;
				} else {
					index = lastPartitionMeta.getNextOffset();
				}*/
				PullResult result = null;
				//do {
					if (lastPartitionMeta == null) {
						index = consumer.fetchConsumeOffset(mq, true);
						index = index == -1 ? 0 : index;
					} else {
						index = lastPartitionMeta.getNextOffset();
					}
					//try {
						result = consumer.pullBlockIfNotFound(mq, config.getTopicTag(), index, 32);
/*						if (result.getPullStatus() == PullStatus.FOUND || result.getPullStatus() == PullStatus.OFFSET_ILLEGAL)
							break;
					} catch (RemotingTimeoutException e) {
					}
				} while (true);*/
				
				List<MessageExt> msgs = result.getMsgFoundList();
				if (null != msgs && msgs.size() > 0) {
					batchMessages = new BatchMessage(msgs, mq);
					batchMessages.setOffset(index);
					//batchMessages.setNextOffset(result.getNextBeginOffset());
					consumer.updateConsumeOffset(mq, result.getMaxOffset());
					for (MessageExt msg : msgs) {
						collector.emit(Lists.newArrayList(tx, msg));
					}
				}
			} catch (MQClientException | RemotingException | MQBrokerException | InterruptedException e) {
				e.printStackTrace();
			}
			return batchMessages;
		}

		private PullResult pullBlockIfNotFound(MessageQueue mq, String subExpression, long offset, int maxNums)
				throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
			do {
				try {
					PullResult res = consumer.pullBlockIfNotFound(mq, subExpression, offset, maxNums);
					if (res.getPullStatus() == PullStatus.FOUND || res.getPullStatus() == PullStatus.OFFSET_ILLEGAL)
						return res;
				} catch (RemotingTimeoutException e) {
				}
			} while (true);
		}

		@Override
		public void refreshPartitions(List<ISpoutPartition> partitionResponsibilities) {

		}

		@Override
		public void emitPartitionBatch(TransactionAttempt tx, TridentCollector collector, ISpoutPartition partition, BatchMessage partitionMeta) {

			MessageQueue mq = null;
			try {
				mq = getMessageQueue(config.getTopic()).get(Integer.parseInt(partition.getId()));

				PullResult result = consumer.pullBlockIfNotFound(mq, config.getTopicTag(), partitionMeta.getOffset(), partitionMeta.getMsgList().size());
				List<MessageExt> msgs = result.getMsgFoundList();
				if (null != msgs && msgs.size() > 0) {
					consumer.updateConsumeOffset(mq, partitionMeta.getNextOffset());
					for (MessageExt msg : msgs) {
						collector.emit(Lists.newArrayList(tx, msg));
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

		}

		@Override
		public void close() {
			LOG.info("close emitter!");
		}

	}

	@Override
	public Coordinator<List<MessageQueue>> getCoordinator(@SuppressWarnings("rawtypes") Map conf, TopologyContext context) {
		return new RocketMQCoordinator();
	}

	@Override
	public Emitter<List<MessageQueue>, ISpoutPartition, BatchMessage> getEmitter(@SuppressWarnings("rawtypes") Map conf, TopologyContext context) {
		return new RocketMQEmitter();
	}

	@SuppressWarnings("rawtypes")
	@Override
	public Map getComponentConfiguration() {
		return null;
	}

	@Override
	public Fields getOutputFields() {
		return new Fields("tId", "message");
	}

}
