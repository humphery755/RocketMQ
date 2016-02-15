package com.alibaba.rocketmq.store.transaction;

public interface TransactionRecord {
    // Message Size
/*    private int msgSize;
    // Timestamp
    int timestamp;
    // Commit Log Offset
    private long offset;

    private long tranStateOffset;
    // Producer Group Hashcode
    private int pgroupHashCode;*/


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
