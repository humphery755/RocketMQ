/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.example.benchmark;

import java.io.FileInputStream;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.remoting.exception.RemotingException;

/**
 * 性能测试，多线程同步发送消息
 */
public class ProducerFixed {
	private static Properties sysConfig = new Properties();
	private static String topic;
	private static String msgBody;
	private static int msgCountLimits = -1;

	public static void main(String[] args) throws MQClientException {
		final int threadCount = args.length >= 1 ? Integer.parseInt(args[0]) : 64;
		final int messageSize = args.length >= 2 ? Integer.parseInt(args[1]) : 128;
		final boolean keyEnable = args.length >= 3 ? Boolean.parseBoolean(args[2]) : false;

		System.out.printf("threadCount %d messageSize %d keyEnable %s\n", threadCount, messageSize, keyEnable);
		try {
			sysConfig.load(new FileInputStream("./init.properties"));
			topic = sysConfig.getProperty("mq.topic", "BenchmarkTest");
			msgBody = sysConfig.getProperty("mq.msgBody");
			msgCountLimits = Integer.valueOf(sysConfig.getProperty("mq.msgCountLimits", "-1"));
		} catch (Exception e1) {
			e1.printStackTrace();
		}

		final ExecutorService sendThreadPool = Executors.newFixedThreadPool(threadCount);

		final StatsBenchmarkProducer statsBenchmark = new StatsBenchmarkProducer();

		final Timer timer = new Timer("BenchmarkTimerThread", true);

		final LinkedList<Long[]> snapshotList = new LinkedList<Long[]>();

		timer.scheduleAtFixedRate(new TimerTask() {
			@Override
			public void run() {
				snapshotList.addLast(statsBenchmark.createSnapshot());
				if (snapshotList.size() > 10) {
					snapshotList.removeFirst();
				}
			}
		}, 1000, 1000);

		timer.scheduleAtFixedRate(new TimerTask() {
			private void printStats() {
				if (snapshotList.size() >= 10) {
					Long[] begin = snapshotList.getFirst();
					Long[] end = snapshotList.getLast();

					final long sendTps = (long) (((end[3] - begin[3]) / (double) (end[0] - begin[0])) * 1000L);
					final double averageRT = ((end[5] - begin[5]) / (double) (end[3] - begin[3]));

					System.out.printf("Send TPS: %d Max RT: %d Average RT: %7.3f Send Failed: %d Response Failed: %d RT Level: %s\n"//
							, sendTps//
							, statsBenchmark.getSendMessageMaxRT().get()//
							, averageRT//
							, end[2]//
							, end[4]//
							, statsBenchmark.getSendMessageRTLevels().toString());
				}
			}

			@Override
			public void run() {
				try {
					this.printStats();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}, 10000, 10000);

		final DefaultMQProducer producer = new DefaultMQProducer("benchmark_producer");
		producer.setInstanceName(Long.toString(System.currentTimeMillis()));

		producer.setCompressMsgBodyOverHowmuch(Integer.MAX_VALUE);

		producer.setDefaultTopicQueueNums(Integer.valueOf(sysConfig.getProperty("mq.defaultTopicQueueNums")));

		producer.setSendMsgTimeout(Integer.valueOf(sysConfig.getProperty("mq.sendMsgTimeout")));

		producer.start();

		while (true) {
			try {
				Thread.currentThread().sleep(10);
				if(statsBenchmark.getSendRequestSuccessCount().get()>msgCountLimits){
					break;
				}
				final long beginTimestamp = System.currentTimeMillis();
				final Message msg = buildMessage(statsBenchmark.getSendRequestSuccessCount().get());
				if (keyEnable) {
					msg.setKeys(String.valueOf(beginTimestamp / 1000));
				}
				
				SendResult sendResult = producer.send(msg);

				final long currentRT = System.currentTimeMillis() - beginTimestamp;
				if (sendResult != null) {
					statsBenchmark.getSendRequestSuccessCount().incrementAndGet();
					statsBenchmark.getReceiveResponseSuccessCount().incrementAndGet();
					statsBenchmark.getSendMessageSuccessTimeTotal().addAndGet(currentRT);
				}
				long prevMaxRT = statsBenchmark.getSendMessageMaxRT().get();
				while (currentRT > prevMaxRT) {
					boolean updated = statsBenchmark.getSendMessageMaxRT().compareAndSet(prevMaxRT, currentRT);
					if (updated)
						break;

					prevMaxRT = statsBenchmark.getSendMessageMaxRT().get();
				}
				statsBenchmark.putResponseTime(currentRT);
			} catch (RemotingException e) {
				statsBenchmark.getSendRequestFailedCount().incrementAndGet();
				e.printStackTrace();

				try {
					Thread.sleep(3000);
				} catch (InterruptedException e1) {
				}
			} catch (InterruptedException e) {
				statsBenchmark.getSendRequestFailedCount().incrementAndGet();
				try {
					Thread.sleep(3000);
				} catch (InterruptedException e1) {
				}
			} catch (MQClientException e) {
				statsBenchmark.getSendRequestFailedCount().incrementAndGet();
				e.printStackTrace();
			} catch (MQBrokerException e) {
				statsBenchmark.getReceiveResponseFailedCount().incrementAndGet();
				try {
					Thread.sleep(3000);
				} catch (InterruptedException e1) {
				}
			}
		}
	}

	private static Message buildMessage(final long index) {
		Message msg = new Message();
		msg.setTopic(topic);

		String strmsg=msgBody+",\"id\":"+index+"}";
		msg.setBody(strmsg.getBytes());

		return msg;
	}
}
