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

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.LocalTransactionExecuter;
import com.alibaba.rocketmq.client.producer.LocalTransactionState;
import com.alibaba.rocketmq.client.producer.TransactionCheckListener;
import com.alibaba.rocketmq.client.producer.TransactionMQProducer;
import com.alibaba.rocketmq.client.producer.TransactionSendResult;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageExt;


/**
 * 性能测试，多线程同步发送事务消息
 */
public class TransactionProducer {
    private static int threadCount;
    private static int messageSize;
    private static int interval;
    private static boolean ischeck;
    private static boolean ischeckffalse;
    private static Properties sysConfig=new Properties();
    private static String topic;
    private static String msgBody;

    public static void main(String[] args) throws MQClientException {
        threadCount = args.length >= 1 ? Integer.parseInt(args[0]) : 32;
        messageSize = args.length >= 2 ? Integer.parseInt(args[1]) : 1024 * 2;
        ischeck = args.length >= 3 ? Boolean.parseBoolean(args[2]) : false;
        ischeckffalse = args.length >= 4 ? Boolean.parseBoolean(args[3]) : false;
        interval=args.length >= 5 ? Integer.parseInt(args[4]) : 0;
        try {
			sysConfig.load(new FileInputStream("./init.properties"));
			topic=sysConfig.getProperty("mq.topic", "BenchmarkTest");
			msgBody=sysConfig.getProperty("mq.msgBody");
		} catch (Exception e1) {
			e1.printStackTrace();
		}
        
        final Message msg = buildMessage(messageSize);

        final StatsBenchmarkTProducer statsBenchmark = new StatsBenchmarkTProducer();

        final Timer timer = new Timer("BenchmarkTimerThread", true);

        final LinkedList<Long[]> snapshotList = new LinkedList<Long[]>();

        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                snapshotList.addLast(statsBenchmark.createSnapshot());
                while (snapshotList.size() > 10) {
                    snapshotList.removeFirst();
                }
            }
        }, 1000, 1000);

        timer.scheduleAtFixedRate(new TimerTask() {
            private void printStats() {
                if (snapshotList.size() >= 10) {
                    Long[] begin = snapshotList.getFirst();
                    Long[] end = snapshotList.getLast();

                    final long sendTps =
                            (long) (((end[3] - begin[3]) / (double) (end[0] - begin[0])) * 1000L);
                    final double averageRT = ((end[5] - begin[5]) / (double) (end[3] - begin[3]));

                    System.out.printf(
                        "Send TPS: %d Max RT: %d Average RT: %7.3f Send Failed: %d Response Failed: %d transaction checkCount: %d RT Level: %s \n"//
                        , sendTps//
                        , statsBenchmark.getSendMessageMaxRT().get()//
                        , averageRT//
                        , end[2]//
                        , end[4]//
                        , end[6]
                        ,statsBenchmark.getSendMessageRTLevels().toString());
                }
            }


            @Override
            public void run() {
                try {
                    this.printStats();
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }, 10000, 10000);

        final TransactionCheckListener transactionCheckListener =
                new TransactionCheckListenerBImpl(ischeckffalse, statsBenchmark);
        final TransactionMQProducer producer = new TransactionMQProducer("benchmark_transaction_producer");
        producer.setInstanceName(Long.toString(System.currentTimeMillis()));
        producer.setTransactionCheckListener(transactionCheckListener);
		
 	// 事务回查最小并发数
		producer.setCheckThreadPoolMinSize(Integer.valueOf(sysConfig.getProperty("mq.checkThreadPoolMinSize")));
		// 事务回查最大并发数
		producer.setCheckThreadPoolMaxSize(Integer.valueOf(sysConfig.getProperty("mq.checkThreadPoolMaxSize")));
		// 队列数
		producer.setCheckRequestHoldMax(Integer.valueOf(sysConfig.getProperty("mq.checkRequestHoldMax")));
		
		producer.setDefaultTopicQueueNums(Integer.valueOf(sysConfig.getProperty("mq.defaultTopicQueueNums")));
		
		producer.setSendMsgTimeout(Integer.valueOf(sysConfig.getProperty("mq.sendMsgTimeout")));
        producer.start();

        final TransactionExecuterBImpl tranExecuter = new TransactionExecuterBImpl(ischeck);
        if(threadCount>0){
        final ExecutorService sendThreadPool = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            sendThreadPool.execute(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        try {
                        	if(interval>0)
								try {
									Thread.sleep(interval);
								} catch (InterruptedException e) {
									e.printStackTrace();
								}
                            final long beginTimestamp = System.currentTimeMillis();
                            TransactionSendResult sendResult =
                                    producer.sendMessageInTransaction(msg, tranExecuter, null);
                            if (sendResult != null) {
                                statsBenchmark.getSendRequestSuccessCount().incrementAndGet();
                                statsBenchmark.getReceiveResponseSuccessCount().incrementAndGet();
                            }

                            final long currentRT = System.currentTimeMillis() - beginTimestamp;
                            statsBenchmark.getSendMessageSuccessTimeTotal().addAndGet(currentRT);
                            long prevMaxRT = statsBenchmark.getSendMessageMaxRT().get();
                            while (currentRT > prevMaxRT) {
                                boolean updated =
                                        statsBenchmark.getSendMessageMaxRT().compareAndSet(prevMaxRT,
                                            currentRT);
                                if (updated)
                                    break;

                                prevMaxRT = statsBenchmark.getSendMessageMaxRT().get();
                            }
                            statsBenchmark.putResponseTime(currentRT);
                        }
                        catch (MQClientException e) {
                            statsBenchmark.getSendRequestFailedCount().incrementAndGet();
                            e.printStackTrace();
                        }
                    }
                }
            });
        }
        }
    }


    private static Message buildMessage(final int messageSize) {
        Message msg = new Message();
        msg.setTopic(topic);

        if(msgBody==null){
	        StringBuilder sb = new StringBuilder();
	        for (int i = 0; i < messageSize; i += 10) {
	            sb.append("hello baby");
	        }
	
	        msg.setBody(sb.toString().getBytes());
        }else{
        	msg.setBody(msgBody.getBytes());
        }
        return msg;
    }
}


class TransactionExecuterBImpl implements LocalTransactionExecuter {

    private boolean ischeck;


    public TransactionExecuterBImpl(boolean ischeck) {
        this.ischeck = ischeck;
    }


    @Override
    public LocalTransactionState executeLocalTransactionBranch(final Message msg, final Object arg) {
    	 try {
			Thread.sleep(600);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
        if (ischeck) {
            return LocalTransactionState.UNKNOW;
        }
        return LocalTransactionState.COMMIT_MESSAGE;
    }
}


class TransactionCheckListenerBImpl implements TransactionCheckListener {
    private boolean ischeckffalse;
    private StatsBenchmarkTProducer statsBenchmarkTProducer;


    public TransactionCheckListenerBImpl(boolean ischeckffalse,
            StatsBenchmarkTProducer statsBenchmarkTProducer) {
        this.ischeckffalse = ischeckffalse;
        this.statsBenchmarkTProducer = statsBenchmarkTProducer;
    }


    @Override
    public LocalTransactionState checkLocalTransactionState(MessageExt msg) {
        // System.out.println("server checking TrMsg " + msg.toString());
        statsBenchmarkTProducer.getCheckRequestSuccessCount().incrementAndGet();
        try {
			Thread.sleep(200);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
        if (ischeckffalse) {

            return LocalTransactionState.ROLLBACK_MESSAGE;
        }

        return LocalTransactionState.COMMIT_MESSAGE;
    }
}


class StatsBenchmarkTProducer {
    // 1
    private final AtomicLong sendRequestSuccessCount = new AtomicLong(0L);
    // 2
    private final AtomicLong sendRequestFailedCount = new AtomicLong(0L);
    // 3
    private final AtomicLong receiveResponseSuccessCount = new AtomicLong(0L);
    // 4
    private final AtomicLong receiveResponseFailedCount = new AtomicLong(0L);
    // 5
    private final AtomicLong sendMessageSuccessTimeTotal = new AtomicLong(0L);
    // 6
    private final AtomicLong sendMessageMaxRT = new AtomicLong(0L);
    private final ConcurrentHashMap<Long,AtomicLong> sendMessageRTLevels=new ConcurrentHashMap();
    // 7
    private final AtomicLong checkRequestSuccessCount = new AtomicLong(0L);


    public Long[] createSnapshot() {
        Long[] snap = new Long[] {//
                System.currentTimeMillis(),//
                        this.sendRequestSuccessCount.get(),//
                        this.sendRequestFailedCount.get(),//
                        this.receiveResponseSuccessCount.get(),//
                        this.receiveResponseFailedCount.get(),//
                        this.sendMessageSuccessTimeTotal.get(), //
                        this.checkRequestSuccessCount.get(), };

        return snap;
    }


    public AtomicLong getSendRequestSuccessCount() {
        return sendRequestSuccessCount;
    }


    public AtomicLong getSendRequestFailedCount() {
        return sendRequestFailedCount;
    }


    public AtomicLong getReceiveResponseSuccessCount() {
        return receiveResponseSuccessCount;
    }


    public AtomicLong getReceiveResponseFailedCount() {
        return receiveResponseFailedCount;
    }


    public AtomicLong getSendMessageSuccessTimeTotal() {
        return sendMessageSuccessTimeTotal;
    }


    public AtomicLong getSendMessageMaxRT() {
        return sendMessageMaxRT;
    }


    public AtomicLong getCheckRequestSuccessCount() {
        return checkRequestSuccessCount;
    }
    
    public ConcurrentHashMap<Long, AtomicLong> getSendMessageRTLevels() {
		return sendMessageRTLevels;
	}


	public void putResponseTime(long currentRT){
    	Long lev=currentRT/1000;
    	AtomicLong oldAtomic = sendMessageRTLevels.get(lev);
    	if(oldAtomic==null){
    		AtomicLong rt = new AtomicLong(1);
    		oldAtomic =sendMessageRTLevels.putIfAbsent(lev, rt);
    		if (oldAtomic==null){
    			oldAtomic=rt;
    		}
    	}
    	oldAtomic.incrementAndGet();
    }
}
