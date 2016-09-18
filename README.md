### RocketMQ

* 基于https://github.com/alibaba/RocketMQ 3.2.6
* 增加了对事务的支持（JDBC事务、事务日志两种实现）
* 增加Paxos,提供对主从Failover的支持（实现中）

----------
-- Broker部署
* vi broker-template.properties
*  namesrvAddr/brokerClusterName/brokerName/zkServer
* brokerId >0且每个部署节点值不同
* brokerRole=SYNC_MASTER
* storePathRootDir/storePathCommitLog 指向共享存储目录
* autoCreateSubscriptionGroup/autoCreateTopicEnable 生产环境均为false
* messageDelayLevel 时间单位: s、m、h、d，分别表示秒、分、时、天
* oracle数据库配置: 
* jdbcDriverClass=oracle.jdbc.driver.OracleDriver
* jdbcURL/jdbcUser/jdbcPassword
* SQL脚本：
* create table T_TRANSACTION( offset NUMBER(20) not null,  pgrouphashcode NUMBER(10),  msgsize NUMBER(10),  timestamp NUMBER(14));
* alter table T_TRANSACTION add primary key (OFFSET);
*
* vi wrapper.conf
* wrapper.name/wrapper.workdir/-Djava.library.path

-- Namesrv部署
* vi namesrv.properties
* myid 每个节点值不同
* rocketmqHome/namesrvAddr/zkServer

* vi wrapper.conf
* wrapper.name/wrapper.workdir/-Djava.library.path