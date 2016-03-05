package com.alibaba.rocketmq.namesrv;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;

import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.namesrv.NamesrvConfig;
import com.alibaba.rocketmq.remoting.netty.NettyServerConfig;
import com.alibaba.rocketmq.srvutil.ServerUtil;

public class PaxosControllerTest {
	PaxosController controller = null;
	String[] args;

	public PaxosControllerTest(String args0, String args1, String args2) {
		args = new String[] { args0, args1, args2 };
	}

	public void setUp() throws Exception {
		// 解析命令行
		Options options = ServerUtil.buildCommandlineOptions(new Options());
		CommandLine commandLine = ServerUtil.parseCmdLine("mqnamesrv", args,
				NamesrvStartup.buildCommandlineOptions(options), new PosixParser());

		// 初始化配置文件
		final NamesrvConfig namesrvConfig = new NamesrvConfig();
		final NettyServerConfig nettyServerConfig = new NettyServerConfig();
		nettyServerConfig.setListenPort(9876);
		if (commandLine.hasOption('c')) {
			String file = commandLine.getOptionValue('c');
			if (file != null) {
				InputStream in = new BufferedInputStream(new FileInputStream(file));
				Properties properties = new Properties();
				properties.load(in);
				MixAll.properties2Object(properties, namesrvConfig);
				MixAll.properties2Object(properties, nettyServerConfig);
				System.out.println("load config properties file OK, " + file);
				in.close();
			}
		}

		// 打印默认配置
		if (commandLine.hasOption('p')) {
			MixAll.printObjectProperties(null, namesrvConfig);
			MixAll.printObjectProperties(null, nettyServerConfig);
			System.exit(0);
		}

		MixAll.properties2Object(ServerUtil.commandLine2Properties(commandLine), namesrvConfig);

		// 初始化服务控制对象
		final NamesrvController namesrvController = new NamesrvController(namesrvConfig, nettyServerConfig);
		namesrvController.initialize();
		controller = namesrvController.getPaxosController();

	}

	public void test() throws Exception {
		controller.start();
	}

	public void tearDown() throws Exception {
		synchronized (PaxosControllerTest.class) {
			while (true) {
				try {
					PaxosControllerTest.class.wait();
				} catch (Throwable e) {
				}
			}
		}
	}
}
