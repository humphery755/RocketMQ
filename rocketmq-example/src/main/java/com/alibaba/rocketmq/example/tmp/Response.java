package com.alibaba.rocketmq.example.tmp;

import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;

public interface Response {
	void excute(RemotingCommand cmd);
}
