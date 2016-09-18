package com.alibaba.rocketmq.broker.ha;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.CreateMode;
import org.daemon.wrapper.LibC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.common.BrokerConfig;
import com.alibaba.rocketmq.common.constant.LoggerName;

public class HAController {
	private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);
	private final BrokerConfig brokerConfig;
	private final String templateFile;
	private final String configFile;
	private ZkClient zkClient;
	private String tmpLockPath;
	private int brokerRole = -1;// -1 uninit, 0 master, 1..n slave
	private String lockPath;

	public HAController(BrokerConfig brokerConfig, String srcFile, String toFile) {
		this.brokerConfig = brokerConfig;
		this.templateFile = srcFile;
		this.configFile = toFile;
	}

	private void createRecursive(String fullPath) {
		if (zkClient.exists(fullPath))
			return;

		String[] pathParts = fullPath.replaceFirst("/", "").split("/");
		StringBuilder path = new StringBuilder();
		for (String pathElement : pathParts) {
			path.append("/").append(pathElement);
			String pathString = path.toString();
			if (!zkClient.exists(pathString)) {
				zkClient.createPersistent(pathString, null);
			}
		}
	}

	public boolean initialize() {
		if (brokerConfig.getBrokerRole() == null || "SLAVE".equals(brokerConfig.getBrokerRole())) {
			log.info("The broker does not participate in the election[selection role is SLAVE,]");
			return true;
		}
		if (brokerConfig.getZkServer() == null) {
			log.error("zkServer can't is null");
			return false;
		}

		lockPath = brokerConfig.getLockPath() + "/" + brokerConfig.getBrokerClusterName() + "/" + brokerConfig.getBrokerName() + "/lock";
		zkClient = new ZkClient(brokerConfig.getZkServer(), brokerConfig.getConnectionTimeout());
		createRecursive(lockPath);
		return false;
	}

	public void start() throws Exception {
		if (brokerConfig.getBrokerRole() == null || "SLAVE".equals(brokerConfig.getBrokerRole())) {
			try {
				Properties properties = readTemplateFile();
				saveConfigFile(properties,"SLAVE",-1);
				LibC.notifydp();
			} catch (Throwable e) {
				e.printStackTrace();
				System.exit(-1);
			}
			return;
		}
		
		tmpLockPath = zkClient.create(lockPath + "/lock", System.currentTimeMillis(), CreateMode.EPHEMERAL_SEQUENTIAL);
		zkClient.subscribeChildChanges(lockPath, new IZkChildListener() {

			@Override
			public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
				log.warn("the node {} child be changed ===>{}", parentPath, currentChilds);
				if (currentChilds == null || currentChilds.size() == 0) {
					log.info("<" + parentPath + "> is deleted");
					return;
				}
				Collections.sort(currentChilds);
				String selfPath = tmpLockPath.substring(lockPath.length() + 1);
				int index = currentChilds.indexOf(selfPath);
				try {
					process(index);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});

		List<String> children = zkClient.getChildren(lockPath);
		if (children.size() > 0) {
			Collections.sort(children);
			String selfPath = tmpLockPath.substring(lockPath.length() + 1);
			int index = children.indexOf(selfPath);
			try {
				process(index);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private synchronized void process(int index) {
		// index == 0, 说明thisNode在列表中最小, 当前client获得锁
		
		if (index <  0)return;
		
		if (index == 0 && brokerRole == 0) {
			log.info("Do nothing!  The broker role has not changed [{}]",brokerConfig.getBrokerRole());
			return;
		}
		
		if ("SYNC_MASTER".equals(brokerConfig.getBrokerRole())) {
			if (index > 0 && brokerRole > 0) {
				log.info("Do nothing!  The broker is SLAVE [selection role is SYNC_MASTER]");
				return;
			}
		}
		
		if ("ASYNC_MASTER".equals(brokerConfig.getBrokerRole())) {
			if (index > 0 && brokerRole > 0) {
				log.info("Do nothing!  The broker role has not changed [SLAVE]");
				return;
			}
		}
		
		/**
		 * SYNC_MASTER 采用共享存储解决Master与Slave节点的数据同步问题，因此主节点只需使用ASYNC_MASTER
		 */
		//String strBrokerRole = index==0?brokerConfig.getBrokerRole():"SLAVE";
		String strBrokerRole = index==0?"ASYNC_MASTER":"SLAVE";
		
		try {
			Properties properties = readTemplateFile();
			if ("SYNC_MASTER".equals(brokerConfig.getBrokerRole())) {
				String tmpDir = properties.getProperty("storePathRootDir");
				tmpDir = tmpDir+ File.separator +"_tmp_slave";
				if(index==0){
					deleteDir(new File(tmpDir));
				}else{
					properties.put("storePathRootDir", tmpDir);
					properties.put("storePathCommitLog", tmpDir+ File.separator +"commitlog");
				}
			}
			saveConfigFile(properties,strBrokerRole,index);
			
			LibC.notifydp();
			if (brokerRole > -1) {
				restartWorkProcess();
			}

			brokerRole = index;
		} catch (Throwable e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}
	
	private static boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            //递归删除目录中的子目录下
            for (int i=0; i<children.length; i++) {
                boolean success = deleteDir(new File(dir, children[i]));
                if (!success) {
                    return false;
                }
            }
        }
        // 目录此时为空，可以删除
        return dir.delete();
    }
	
	private Properties readTemplateFile()throws IOException{
		InputStream in = new BufferedInputStream(new FileInputStream(templateFile));
		Properties properties = new Properties();
		properties.load(in);
		in.close();
		return properties;
	}
	private void saveConfigFile(Properties properties,String brokerRole,int index) throws IOException{
		properties.put("brokerRole", brokerRole);
		if(index==0)
			properties.put("brokerId", "0");
		OutputStream ou = new FileOutputStream(configFile);
		properties.store(ou, "brokerRole => "+brokerRole);
		ou.close();
	}

	private void restartWorkProcess() {
		int pid = LibC.getwpid();
		if (pid > 0)
			LibC.kill(pid, 15);
		else {
			LibC.notifydp();
		}
	}

	public void shutdown() {
		if (brokerConfig.getBrokerRole() == null || "SLAVE".equals(brokerConfig.getBrokerRole())) {
			return;
		}
		zkClient.close();
	}

	public BrokerConfig getBrokerConfig() {
		return brokerConfig;
	}
}
