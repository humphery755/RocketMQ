/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.broker.ha;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.daemon.wrapper.LibC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.common.BrokerConfig;
import com.alibaba.rocketmq.common.MQVersion;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.remoting.common.RemotingUtil;
import com.alibaba.rocketmq.remoting.netty.NettySystemConfig;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;
import com.alibaba.rocketmq.srvutil.ServerUtil;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;

/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-26
 */
public class HABrokerStartup implements LibC.Service{
	
	public static CommandLine commandLine = null;
	public static String configFile = null;
	public static Logger log;
	private HAController controller ;

	public static Options buildCommandlineOptions(final Options options) {
		Option opt = new Option("c", "configFile", true, "Broker config properties file");
		opt.setRequired(true);
		options.addOption(opt);

		opt = new Option("t", "templateFile", true, "Broker template config file");
		opt.setRequired(true);
		options.addOption(opt);

		return options;
	}

	public static HAController createBrokerController(String[] args) {
		System.setProperty(RemotingCommand.RemotingVersionKey, Integer.toString(MQVersion.CurrentVersion));

		// Socket发送缓冲区大小
		if (null == System.getProperty(NettySystemConfig.SystemPropertySocketSndbufSize)) {
			NettySystemConfig.SocketSndbufSize = 131072;
		}

		// Socket接收缓冲区大小
		if (null == System.getProperty(NettySystemConfig.SystemPropertySocketRcvbufSize)) {
			NettySystemConfig.SocketRcvbufSize = 131072;
		}

		try {

			// 解析命令行
			Options options = ServerUtil.buildCommandlineOptions(new Options());
			commandLine = ServerUtil.parseCmdLine("mqbroker", args, buildCommandlineOptions(options), new PosixParser());
			if (null == commandLine) {
				System.exit(-1);
				return null;
			}

			// 初始化配置文件
			final BrokerConfig brokerConfig = new BrokerConfig();

			Properties properties=null;
			String templateFile=null;
			// 指定配置文件
			if (commandLine.hasOption('t')) {
				templateFile = commandLine.getOptionValue('t');
				if (templateFile != null) {
					InputStream in = new BufferedInputStream(new FileInputStream(templateFile));
					properties = new Properties();
					properties.load(in);
					MixAll.properties2Object(properties, brokerConfig);

					System.out.println("load config properties file OK, " + templateFile);
					in.close();
				}
			} else {
				System.out.println("please config properties file");
				System.exit(-1);
				return null;
			}
			
			configFile = commandLine.getOptionValue('c');

			MixAll.properties2Object(ServerUtil.commandLine2Properties(commandLine), brokerConfig);

			if (null == brokerConfig.getRocketmqHome()) {
				System.out.println("Please set the " + MixAll.ROCKETMQ_HOME_ENV + " variable in your environment to match the location of the RocketMQ installation");
				System.exit(-2);
			}

			// 检测Name Server地址设置是否正确 IP:PORT
			String namesrvAddr = brokerConfig.getNamesrvAddr();
			if (null != namesrvAddr) {
				try {
					String[] addrArray = namesrvAddr.split(";");
					if (addrArray != null) {
						for (String addr : addrArray) {
							RemotingUtil.string2SocketAddress(addr);
						}
					}
				} catch (Exception e) {
					System.out.printf("The Name Server Address[%s] illegal, please set it as follows, \"127.0.0.1:9876;192.168.0.1:9876\"\n", namesrvAddr);
					System.exit(-3);
				}
			}

			// BrokerId的处理
			/*
			 * switch (messageStoreConfig.getBrokerRole()) { case ASYNC_MASTER:
			 * case SYNC_MASTER: // Master Id必须是0
			 * brokerConfig.setBrokerId(MixAll.MASTER_ID); break; case SLAVE: if
			 * (brokerConfig.getBrokerId() <= 0) { System.out.println(
			 * "Slave's brokerId must be > 0"); System.exit(-3); }
			 * 
			 * break; default: break; }
			 */

			// 初始化Logback
			System.setProperty("logback.configurationFile", brokerConfig.getRocketmqHome() + "/conf/logback_broker.xml");
			LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
			JoranConfigurator configurator = new JoranConfigurator();
			configurator.setContext(lc);
			lc.reset();
			configurator.doConfigure(brokerConfig.getRocketmqHome() + "/conf/logback_broker.xml");
			log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);
			// 打印启动参数
			MixAll.printObjectProperties(log, brokerConfig);

			HAController controller = new HAController(brokerConfig,templateFile,configFile);
			return controller;
		} catch (Throwable e) {
			e.printStackTrace();
			System.exit(-1);
		}

		return null;
	}

	public static HAController start(HAController controller) {
		try {
			// 启动服务控制对象
			controller.start();
			String tip = "The HAbroker[" + controller.getBrokerConfig().getBrokerName() + "] boot success.";
			log.info(tip);
			System.out.println(tip);

			return controller;
		} catch (Throwable e) {
			e.printStackTrace();
			System.exit(-1);
		}

		return null;
	}

	@Override
	public void init(String[] args) {
		controller=createBrokerController(args);
		controller.initialize();
	}

	@Override
	public void startup() {
		start(controller);
	}

	@Override
	public void destory() {
		controller.shutdown();
	}
}
