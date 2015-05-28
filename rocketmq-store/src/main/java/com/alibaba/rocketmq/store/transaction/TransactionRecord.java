package com.alibaba.rocketmq.store.transaction;

public class TransactionRecord {
    // Message Size
    private int msgSize;
    // Timestamp
    int timestamp;
    // Commit Log Offset
    private long offset;

    private long tranStateOffset;
    // Producer Group Hashcode
    private int pgroupHashCode;


    public long getOffset() {
        return offset;
    }


    public void setOffset(long offset) {
        this.offset = offset;
    }

    public long getTranStateOffset() {
        return tranStateOffset;
    }

    public void setTranStateOffset(long tranStateOffset) {
        this.tranStateOffset = tranStateOffset;
    }

    public int getPgroupHashCode() {
        return pgroupHashCode;
    }

    public void setPgroupHashCode(int pgroupHashCode) {
        this.pgroupHashCode = pgroupHashCode;
    }

    public int getMsgSize() {
        return msgSize;
    }

    public void setMsgSize(int msgSize) {
        this.msgSize = msgSize;
    }

    public int getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(int timestamp) {
        this.timestamp = timestamp;
    }
}
