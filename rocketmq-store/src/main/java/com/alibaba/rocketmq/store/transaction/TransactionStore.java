package com.alibaba.rocketmq.store.transaction;

import java.util.List;


/**
 * 事务存储接口，主要为分布式事务消息存储服务
 */
public interface TransactionStore {
    public boolean start(boolean lastExitOK);

    public boolean put(TransactionRecord trs);


    public void remove(final Long pks);


    //public List<TransactionRecord> traverse(final long pk, final int nums);


    //public long totalRecords();


    //public long minPK();


    //public long maxPK();

    public void shutdown();
}
