package com.alibaba.rocketmq.store.transaction;

import com.alibaba.rocketmq.store.DispatchRequest;

/**
 * 事务存储接口，主要为分布式事务消息存储服务
 */
public interface TransactionStore {
	int PreparedMessageTagsCode = -1;
	public boolean start();
	public void shutdown();
	boolean load();
	public boolean execute(DispatchRequest req);
	public void recoverTranRedoLog(boolean lastExitOK);
	public void recoverStateTable(boolean lastExitOK);
	public int deleteExpiredRedoLogFile(long minOffset);
	public int deleteExpiredStateFile(long minOffset);
	public void commitRedoLog(int flushConsumeQueueLeastPages);
	public long getTranStateTableOffset();
	public long incrementTranStateTableOffset();
}
