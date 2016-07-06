package com.alibaba.rocketmq.storm.domain;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;

/**
 * @author Von Gosling
 */
public class BatchMessage {
    private final static long WAIT_TIMEOUT = 4;

    private UUID              batchId;
    private List<MessageExt>  msgList;
    private MessageQueue      mq;

    private CountDownLatch    latch;
    private boolean           isSuccess;

    private long              nextOffset;
    private long              offset;

    private MessageStat       messageStat;

    public BatchMessage() {
    }

    public BatchMessage(List<MessageExt> msgList, MessageQueue mq) {
        this.msgList = msgList;
        this.mq = mq;

        this.batchId = UUID.randomUUID();
        this.latch = new CountDownLatch(1);
        this.isSuccess = false;
    }

    public UUID getBatchId() {
        return batchId;
    }

    public List<MessageExt> getMsgList() {
        return msgList;
    }

    public boolean waitFinish() throws InterruptedException {
        return latch.await(WAIT_TIMEOUT, TimeUnit.HOURS);
    }

    public void done() {
        isSuccess = true;
        latch.countDown();
    }

    public void fail() {
        isSuccess = false;
        latch.countDown();
    }

    public boolean isSuccess() {
        return isSuccess;
    }

    public String getMessageQueue() {
        if (mq == null) {
            return null;
        }
        return mq.toString();
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public long getNextOffset() {
        return nextOffset;
    }

    public void setNextOffset(long nextOffset) {
        this.nextOffset = nextOffset;
    }

    public MessageStat getMessageStat() {
        return messageStat;
    }

    public void setMessageStat(MessageStat messageStat) {
        this.messageStat = messageStat;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }

}
