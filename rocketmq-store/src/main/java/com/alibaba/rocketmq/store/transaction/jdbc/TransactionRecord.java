package com.alibaba.rocketmq.store.transaction.jdbc;

public interface TransactionRecord {
    public long getOffset();

    public void setOffset(long offset);

    public long getTranStateOffset();

    public void setTranStateOffset(long tranStateOffset);

    public int getPgroupHashCode();

    public void setPgroupHashCode(int pgroupHashCode) ;
    public int getMsgSize() ;

    public void setMsgSize(int msgSize) ;

    public int getTimestamp();

    public void setTimestamp(int timestamp);
}
