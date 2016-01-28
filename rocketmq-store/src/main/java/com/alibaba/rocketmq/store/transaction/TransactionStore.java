package com.alibaba.rocketmq.store.transaction;

/**
 * 事务存储接口，主要为分布式事务消息存储服务
 */
public interface TransactionStore {
	public boolean start(boolean lastExitOK);

	public boolean put(TransactionRecord trs);

	public boolean update(//
			final long tsOffset, //
			final long clOffset, //
			final int groupHashCode, //
			final int state//
	);

	void gotoCheck(int producerGroupHashCode, long tranStateTableOffset, long commitLogOffset, int msgSize);
	// public List<TransactionRecord> traverse(final long pk, final int nums);

	// public long totalRecords();

	// public long minPK();

	// public long maxPK();

	public void shutdown();
}
