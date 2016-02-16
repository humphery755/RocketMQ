package com.alibaba.rocketmq.store.transaction.jdbc;

import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.message.MessageConst;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.sysflag.MessageSysFlag;
import com.alibaba.rocketmq.store.DefaultMessageStore;
import com.alibaba.rocketmq.store.DispatchRequest;
import com.alibaba.rocketmq.store.config.BrokerRole;
import com.alibaba.rocketmq.store.transaction.TransactionStore;

import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;

/**
 * @author humphery
 */
public class JDBCTransactionStore implements TransactionStore {
	private static final Logger log = LoggerFactory.getLogger(LoggerName.TransactionLoggerName);
	private Connection connection;
	private AtomicLong totalRecordsValue = new AtomicLong(0);
	private final DefaultMessageStore messageStore;
	// 定时回查线程
	private final Timer timer = new Timer("CheckTransactionMessageTimer", true);
	// TODO: 2G堆外内存约可承载5000万prepareTransaction？ 2G/36B
	private final ChronicleMap<Long, TransactionRecord> tranStateTable;
	// State Table Offset，重启时，必须纠正
	private final AtomicLong tranStateTableOffset = new AtomicLong(0);

	public JDBCTransactionStore(DefaultMessageStore messageStore) {
		this.messageStore = messageStore;
		ChronicleMapBuilder<Long, TransactionRecord> tranStateMapBuilder = ChronicleMapBuilder
				.of(Long.class, TransactionRecord.class).entries(500_000);
		tranStateTable = tranStateMapBuilder.create();
	}

	private boolean computeTotalRecords() {
		Statement statement = null;
		ResultSet resultSet = null;
		try {
			statement = this.connection.createStatement();

			resultSet = statement.executeQuery("select count(offset) as total from t_transaction");
			if (!resultSet.next()) {
				log.warn("computeTotalRecords ResultSet is empty");
				return false;
			}

			this.totalRecordsValue.set(resultSet.getLong(1));
		} catch (Exception e) {
			log.warn("computeTotalRecords Exception", e);
			return false;
		} finally {
			if (null != statement) {
				try {
					statement.close();
				} catch (SQLException e) {
				}
			}

			if (null != resultSet) {
				try {
					resultSet.close();
				} catch (SQLException e) {
				}
			}
		}

		return true;
	}

	private String createTableSql() {
		URL resource = JDBCTransactionStore.class.getClassLoader().getResource("transaction.sql");
		String fileContent = MixAll.file2String(resource);
		return fileContent;
	}

	private boolean createDB() {
		Statement statement = null;
		try {
			statement = this.connection.createStatement();

			String sql = this.createTableSql();
			log.info("createDB SQL:\n {}", sql);
			statement.execute(sql);
			this.connection.commit();
			return true;
		} catch (Exception e) {
			log.warn("createDB Exception", e);
			return false;
		} finally {
			if (null != statement) {
				try {
					statement.close();
				} catch (SQLException e) {
				}
			}
		}
	}

	@Override
	public boolean start() {
		
		/*
		 * else { // 异常数据恢复，OS CRASH或者JVM CRASH或者机器掉电 tranStateTable.clear(); }
		 */

		addTimerTask();

		return true;
	}

	private long updatedRows(int[] rows) {
		long res = 0;
		for (int i : rows) {
			res += i;
		}

		return res;
	}

	@Override
	public boolean execute(DispatchRequest req) {
		final int tranType = MessageSysFlag.getTransactionValue(req.getSysFlag());
		int groupHashCode = req.getProducerGroup() == null ? PreparedMessageTagsCode
				: req.getProducerGroup().hashCode();
/*		if (groupHashCode == -1)
			log.warn("groupHashCode is -1, StackTrace: {}", UtilAll.currentStackTrace());*/
		switch (tranType) {
		case MessageSysFlag.TransactionNotType:
			break;
		case MessageSysFlag.TransactionPreparedType:
			TransactionRecord tr = tranStateTable.newValueInstance();
			tr.setOffset(req.getCommitLogOffset());
			tr.setTranStateOffset(req.getTranStateTableOffset());

			tr.setPgroupHashCode(groupHashCode);
			tr.setMsgSize(req.getMsgSize());
			tr.setTimestamp((int) (req.getStoreTimestamp() / 1000));
			tranStateTable.put(req.getCommitLogOffset(), tr);
			return true;
		case MessageSysFlag.TransactionCommitType:
		case MessageSysFlag.TransactionRollbackType:
			return tranStateTable.remove(req.getPreparedTransactionOffset()) != null;
		}

		return true;
	}

	private void addTimerTask() {
		this.timer.scheduleAtFixedRate(new TimerTask() {
			private final long checkTransactionMessageAtleastInterval = JDBCTransactionStore.this.messageStore
					.getMessageStoreConfig().getCheckTransactionMessageAtleastInterval();
			private final boolean slave = JDBCTransactionStore.this.messageStore.getMessageStoreConfig()
					.getBrokerRole() == BrokerRole.SLAVE;

			@Override
			public void run() {
				// Slave不需要回查事务状态
				if (slave)
					return;

				try {
					int preparedMessageCount = 0;
					int total=tranStateTable.size();
					for (Map.Entry<Long, TransactionRecord> entry : tranStateTable.entrySet()) {
						TransactionRecord tr = entry.getValue();

						// 遇到时间不符合，终止
						long timestampLong = Long.valueOf(tr.getTimestamp()) * 1000;
						long diff = System.currentTimeMillis() - timestampLong;
						if (diff < checkTransactionMessageAtleastInterval) {
							// break;
							continue;
						}

						preparedMessageCount++;

						try {
							
							messageStore.getTransactionCheckExecuter().gotoCheck(//
									tr.getPgroupHashCode(), // Producer Group
															// Hashcode
									tr.getTranStateOffset(), // getTranStateOffset(i),//Transaction
																// State Table
																// Offset ==
																// consumer
																// queue offset
									tr.getOffset(), // Commit Log Offset
									tr.getMsgSize()// Message Size
							);
						} catch (Exception e) {
							log.error("gotoCheck Exception", e);
						}
					}

					// 无Prepared消息，且遍历完，则终止定时任务
					/*
					 * if (0 == preparedMessageCountInThisMapedFile) { log.info(
					 * "remove the transaction timer task, because no prepared message in this mapedfile[{}]"
					 * ); this.cancel(); }
					 */

					log.info(
							"the transaction timer task execute over in this period, Prepared Message: {} Check Progress: {}/{}",preparedMessageCount,tranStateTable.size(),total
					// mapedFile.getFileName(),//
					// preparedMessageCountInThisMapedFile//
					// i / TSStoreUnitSize,//
					// mapedFile.getFileSize() / TSStoreUnitSize//
					);

				} catch (Exception e) {
					log.error("check transaction timer task Exception", e);
				}
			}

			/*
			 * private long getTranStateOffset(final long currentIndex) { long
			 * offset = (this.mapedFile.getFileFromOffset() + currentIndex) /
			 * TransactionStateService.TSStoreUnitSize; return offset; }
			 */
		}, 1000 * 60, this.messageStore.getMessageStoreConfig().getCheckTransactionMessageTimerInterval());
	}

	public void shutdown() {
		Statement statement = null;
		try {
			statement = this.connection.createStatement();
			statement.execute("truncate table t_transaction");
			this.connection.commit();
		} catch (Exception e) {
			log.warn("truncate table t_transaction Exception", e);
			return;
		} finally {
			if (null != statement) {
				try {
					statement.close();
				} catch (SQLException e) {
				}
			}
		}

		PreparedStatement pstatement = null;
		try {
			this.connection.setAutoCommit(false);
			pstatement = this.connection.prepareStatement("insert into t_transaction values (?, ?, ?, ?)");
			int i = 0;
			for (TransactionRecord tr : tranStateTable.values()) {
				pstatement.setLong(1, tr.getOffset());
				pstatement.setInt(2, tr.getPgroupHashCode());
				pstatement.setInt(3, tr.getMsgSize());
				pstatement.setInt(4, tr.getTimestamp());
				pstatement.addBatch();
				i++;
				// 分段提交
				if (i == 50000) {
					int[] executeBatch = pstatement.executeBatch();
					this.connection.commit();
					this.totalRecordsValue.addAndGet(updatedRows(executeBatch));
					i = 0;
				}
			}
			if (i > 0) {
				int[] executeBatch = pstatement.executeBatch();
				this.connection.commit();
				this.totalRecordsValue.addAndGet(updatedRows(executeBatch));
			}
		} catch (Exception e) {
			log.warn("insert into t_transaction Exception", e);
			return;
		} finally {
			if (null != pstatement) {
				try {
					pstatement.close();
				} catch (SQLException e) {
				}
			}
			tranStateTable.clear();
		}

		try {
			if (this.connection != null) {
				this.connection.close();
			}
		} catch (SQLException e) {
		}
	}

	@Override
	public boolean load() {
		try {
			Class.forName(this.messageStore.getMessageStoreConfig().getJdbcDriverClass()).newInstance();
			log.info("Loaded the appropriate driver, {}",
					this.messageStore.getMessageStoreConfig().getJdbcDriverClass());
		} catch (Exception e) {
			log.info("Loaded the appropriate driver Exception", e);
			return false;
		}
		Properties props = new Properties();
		props.put("user", messageStore.getMessageStoreConfig().getJdbcUser());
		props.put("password", messageStore.getMessageStoreConfig().getJdbcPassword());

		try {
			this.connection = DriverManager.getConnection(this.messageStore.getMessageStoreConfig().getJdbcURL(),
					props);

			this.connection.setAutoCommit(false);

			// 如果表不存在，尝试初始化表
			if (!this.computeTotalRecords()) {
				return this.createDB();
			}

		} catch (SQLException e) {
			log.error("Create JDBC Connection Exeption", e);
			return false;
		}
		return true;
	}

	@Override
	public void recoverTranRedoLog(boolean lastExitOK) {
		// 正常数据恢复
		if (lastExitOK) {
			Statement statement = null;
			ResultSet resultSet = null;
			try {
				statement = JDBCTransactionStore.this.connection.createStatement();
				resultSet = statement.executeQuery("select offset,pgrouphashcode,msgsize,timestamp from t_transaction");
				while (resultSet.next()) {
					TransactionRecord tr = tranStateTable.newValueInstance();
					tr.setOffset(resultSet.getLong(1));
					tr.setPgroupHashCode(resultSet.getInt(2));
					tr.setMsgSize(resultSet.getInt(3));
					tr.setTimestamp(resultSet.getInt(4));
					tranStateTable.put(tr.getOffset(), tr);
				}
			} catch (Exception e) {
				log.warn("computeTotalRecords Exception", e);
				return;
			} finally {
				if (null != statement) {
					try {
						statement.close();
					} catch (SQLException e) {
					}
				}

				if (null != resultSet) {
					try {
						resultSet.close();
					} catch (SQLException e) {
					}
				}
			}
		}
	}

	@Override
	public void recoverStateTable(boolean lastExitOK) {
		//if (!lastExitOK) {
			for (TransactionRecord tr : tranStateTable.values()) {
				MessageExt msgExt = this.messageStore.lookMessageByOffset(tr.getOffset());
				if (msgExt != null) {
					if (lastExitOK) {
						tr.setTranStateOffset(msgExt.getQueueOffset());
					}
					if(tr.getTranStateOffset()!=msgExt.getQueueOffset()){
						log.warn("tranStateOffset error recover TranStateOffset: {} and commitLog's TranStateOffset: {}", tr.getTranStateOffset(),msgExt.getQueueOffset());
					}
					tr.setPgroupHashCode(msgExt.getProperty(MessageConst.PROPERTY_PRODUCER_GROUP).hashCode());
					if(tranStateTableOffset.get()<tr.getTranStateOffset())
						this.tranStateTableOffset.set(tr.getTranStateOffset()+1);
				}else{
					log.error("Offset error not found Message by offset: {}", tr.getOffset());
				}
			}
		//}
	}

	@Override
	public int deleteExpiredRedoLogFile(long minOffset) {
		return 0;
	}

	@Override
	public int deleteExpiredStateFile(long minOffset) {
		return 0;
	}

	@Override
	public void commitRedoLog(int flushConsumeQueueLeastPages) {
		// TODO Auto-generated method stub

	}

	@Override
	public long getTranStateTableOffset() {
		return tranStateTableOffset.get();
	}

	@Override
	public long incrementTranStateTableOffset() {
		return tranStateTableOffset.incrementAndGet();
	}
}
