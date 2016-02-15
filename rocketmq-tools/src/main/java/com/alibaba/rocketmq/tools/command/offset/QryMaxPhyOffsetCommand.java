package com.alibaba.rocketmq.tools.command.offset;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import com.alibaba.rocketmq.tools.admin.DefaultMQAdminExt;
import com.alibaba.rocketmq.tools.command.SubCommand;


/**
 * 根据时间来设置消费进度，设置之前要关闭这个订阅组的所有consumer，设置完再启动，方可生效。
 * 
 * @author: manhong.yqd<jodie.yqd@gmail.com>
 * @since: 13-9-12
 */
public class QryMaxPhyOffsetCommand implements SubCommand {
    @Override
    public String commandName() {
        return "qryMaxPhyOffset";
    }


    @Override
    public String commandDesc() {
        return "Query Max offset by commitlog).";
    }


    @Override
    public Options buildCommandlineOptions(Options options) {
        return options;
    }


    public static void queryOffset(DefaultMQAdminExt defaultMQAdminExt) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException {
        /*List<RollbackStats> rollbackStatsList =
                defaultMQAdminExt.g(consumerGroup, topic, timestamp, force);
        System.out
            .printf(
                "rollback consumer offset by specified consumerGroup[%s], topic[%s], force[%s], timestamp(string)[%s], timestamp(long)[%s]\n",
                consumerGroup, topic, force, timeStampStr, timestamp);

        System.out.printf("%-20s  %-20s  %-20s  %-20s  %-20s  %-20s\n",//
            "#brokerName",//
            "#queueId",//
            "#brokerOffset",//
            "#consumerOffset",//
            "#timestampOffset",//
            "#rollbackOffset" //
        );

        for (RollbackStats rollbackStats : rollbackStatsList) {
            System.out.printf("%-20s  %-20d  %-20d  %-20d  %-20d  %-20d\n",//
                UtilAll.frontStringAtLeast(rollbackStats.getBrokerName(), 32),//
                rollbackStats.getQueueId(),//
                rollbackStats.getBrokerOffset(),//
                rollbackStats.getConsumerOffset(),//
                rollbackStats.getTimestampOffset(),//
                rollbackStats.getRollbackOffset() //
                );*/
    }


    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        try {
            String offsetStr = commandLine.getOptionValue("o").trim();

            boolean force = true;
            if (commandLine.hasOption('f')) {
                force = Boolean.valueOf(commandLine.getOptionValue("f").trim());
            }

            defaultMQAdminExt.start();
            queryOffset(defaultMQAdminExt);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
