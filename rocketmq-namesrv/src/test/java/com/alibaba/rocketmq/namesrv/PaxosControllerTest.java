package com.alibaba.rocketmq.namesrv;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.namesrv.NamesrvConfig;
import com.alibaba.rocketmq.remoting.netty.NettyServerConfig;
import com.alibaba.rocketmq.srvutil.ServerUtil;

public class PaxosControllerTest {

	static public class PaxosControllerTestImp {
		PaxosController controller = null;
		String[] args;

		public PaxosControllerTestImp(String args0, String args1, String args2) {
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
			controller = new PaxosController(namesrvController);
			boolean initResult = controller.initialize();

		}

		public void test() throws Exception {
			controller.start();
		}

		public void tearDown() throws Exception {
			synchronized (PaxosControllerTestImp.class) {
				while (true) {
					try {
						PaxosControllerTestImp.class.wait();
					} catch (Throwable e) {
					}
				}
			}
			// controller.shutdown();
		}
	}

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Test
	public void test1() throws Exception {
		new Thread() {
			public void run() {
				PaxosControllerTestImp p=new PaxosControllerTestImp("bin/mqnamesrv", "-c", "../conf/namesrv.properties" );
				try {
					p.setUp();
				} catch (Exception e) {
					e.printStackTrace();
				}
				try {
					p.test();
				} catch (Exception e) {
					e.printStackTrace();
				}
				try {
					p.tearDown();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}.start();
		
		new Thread() {
			public void run() {
				PaxosControllerTestImp p=new PaxosControllerTestImp("bin/mqnamesrv", "-c", "../conf/namesrv1.properties" );
				try {
					p.setUp();
				} catch (Exception e) {
					e.printStackTrace();
				}
				try {
					p.test();
				} catch (Exception e) {
					e.printStackTrace();
				}
				try {
					p.tearDown();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}.start();
	}

	@After
	public void tearDown() throws Exception {
		synchronized (PaxosControllerTest.class) {
			while (true) {
				try {
					PaxosControllerTest.class.wait();
				} catch (Throwable e) {
				}
			}
		}
		// controller.shutdown();
	}

}
