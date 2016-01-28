package com.alibaba.rocketmq.broker.transaction.jdbc;

import com.alibaba.rocketmq.broker.BrokerController;
import com.alibaba.rocketmq.broker.client.ClientChannelInfo;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.protocol.header.CheckTransactionStateRequestHeader;
import com.alibaba.rocketmq.store.SelectMapedBufferResult;
import com.alibaba.rocketmq.store.config.BrokerRole;
import com.alibaba.rocketmq.store.transaction.TransactionRecord;
import com.alibaba.rocketmq.store.transaction.TransactionStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.nio.ByteBuffer;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author humphery
 */
public class JDBCTransactionStore implements TransactionStore {
	private static final Logger log = LoggerFactory.getLogger(LoggerName.TransactionLoggerName);
	private final JDBCTransactionStoreConfig jdbcTransactionStoreConfig;
	private Connection connection;
	private AtomicLong totalRecordsValue = new AtomicLong(0);
	private BrokerController brokerController;
	// 定时回查线程
	private final Timer timer = new Timer("CheckTransactionMessageTimer", true);
	// TODO:未提交事务消息过多时存在内存被撑爆，待优化
	private Map<Long, TransactionRecord> tranStateTable = new ConcurrentHashMap();

	public JDBCTransactionStore(JDBCTransactionStoreConfig jdbcTransactionStoreConfig,
			BrokerController brokerController) {
		this.jdbcTransactionStoreConfig = jdbcTransactionStoreConfig;
		this.brokerController = brokerController;
	}

	private boolean loadDriver() {
		try {
			Class.forName(this.jdbcTransactionStoreConfig.getJdbcDriverClass()).newInstance();
			log.info("Loaded the appropriate driver, {}", this.jdbcTransactionStoreConfig.getJdbcDriverClass());
			return true;
		} catch (Exception e) {
			log.info("Loaded the appropriate driver Exception", e);
		}

		return false;
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
	public boolean start(boolean lastExitOK) {
		if (this.loadDriver()) {
			Properties props = new Properties();
			props.put("user", jdbcTransactionStoreConfig.getJdbcUser());
			props.put("password", jdbcTransactionStoreConfig.getJdbcPassword());

			try {
				this.connection = DriverManager.getConnection(this.jdbcTransactionStoreConfig.getJdbcURL(), props);

				this.connection.setAutoCommit(false);

				// 如果表不存在，尝试初始化表
				if (!this.computeTotalRecords()) {
					return this.createDB();
				}

			} catch (SQLException e) {
				log.error("Create JDBC Connection Exeption", e);
				return false;
			}
		}
		// 正常数据恢复
		if (lastExitOK) {
			Statement statement = null;
			ResultSet resultSet = null;
			try {
				statement = JDBCTransactionStore.this.connection.createStatement();
				resultSet = statement.executeQuery("select offset,pgrouphashcode,msgsize,timestamp from t_transaction");
				while (resultSet.next()) {
					TransactionRecord tr = new TransactionRecord();
					tr.setOffset(resultSet.getLong(1));
					tr.setPgroupHashCode(resultSet.getInt(2));
					tr.setMsgSize(resultSet.getInt(3));
					tr.setTimestamp(resultSet.getInt(4));
					tranStateTable.put(tr.getOffset(), tr);
				}
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
		} /*
			 * else { // 异常数据恢复，OS CRASH或者JVM CRASH或者机器掉电
			 * tranStateTable.clear(); }
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

	// public void remove(List<Long> pks) {
	@Override
	public boolean update(//
            final long tsOffset,//
            final long clOffset,//
            final int groupHashCode,//
            final int state//
    ){
		tranStateTable.remove(clOffset);
		/*
		 * PreparedStatement statement = null; try {
		 * this.connection.setAutoCommit(false); statement =
		 * this.connection.prepareStatement(
		 * "DELETE FROM t_transaction WHERE offset = ?"); for (long pk : pks) {
		 * statement.setLong(1, pk); statement.addBatch(); } int[] executeBatch
		 * = statement.executeBatch();
		 * System.out.println(Arrays.toString(executeBatch));
		 * this.connection.commit(); //this.totalRecordsValue.addAndGet(-
		 * updatedRows(executeBatch)); } catch (Exception e) { log.warn(
		 * "createDB Exception", e); } finally { if (null != statement) { try {
		 * statement.close(); } catch (SQLException e) { } } }
		 */
		return true;
	}

	// @Override
	public long totalRecords() {
		// TODO Auto-generated method stub
		return this.totalRecordsValue.get();
	}

	// @Override
	public long minPK() {
		// TODO Auto-generated method stub
		return 0;
	}

	// @Override
	public long maxPK() {
		// TODO Auto-generated method stub
		return 0;
	}

	// public boolean put(List<TransactionRecord> trs) {
	@Override
	public boolean put(TransactionRecord tr) {
		tranStateTable.put(tr.getOffset(), tr);
		return true;
	}

	private void addTimerTask() {
		this.timer.scheduleAtFixedRate(new TimerTask() {
			private final long checkTransactionMessageAtleastInterval = JDBCTransactionStore.this.jdbcTransactionStoreConfig
					.getCheckTransactionMessageAtleastInterval();
			private final boolean slave = JDBCTransactionStore.this.brokerController.getMessageStoreConfig()
					.getBrokerRole() == BrokerRole.SLAVE;

			@Override
			public void run() {
				// Slave不需要回查事务状态
				if (slave)
					return;

				try {
					// long preparedMessageCountInThisMapedFile = 0;

					for (Map.Entry<Long, TransactionRecord> entry : tranStateTable.entrySet()) {
						TransactionRecord tr = entry.getValue();

						// 遇到时间不符合，终止
						long timestampLong = Long.valueOf(tr.getTimestamp()) * 1000;
						long diff = System.currentTimeMillis() - timestampLong;
						if (diff < checkTransactionMessageAtleastInterval) {
							// break;
							continue;
						}

						// preparedMessageCountInThisMapedFile++;

						try {
							gotoCheck(//
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
							log.warn("gotoCheck Exception", e);
						}
					}

					// 无Prepared消息，且遍历完，则终止定时任务
					/*
					 * if (0 == preparedMessageCountInThisMapedFile) { log.info(
					 * "remove the transaction timer task, because no prepared message in this mapedfile[{}]"
					 * ); this.cancel(); }
					 */

					log.info(
							"the transaction timer task execute over in this period, {} Prepared Message: {} Check Progress: {}/{}"
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
		}, 1000 * 60, this.jdbcTransactionStoreConfig.getCheckTransactionMessageTimerInterval());
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
		}

		try {
			if (this.connection != null) {
				this.connection.close();
			}
		} catch (SQLException e) {
		}
	}

	@Override
	public void gotoCheck(int producerGroupHashCode, long tranStateTableOffset, long commitLogOffset, int msgSize) {
		// 第一步、查询Producer
		final ClientChannelInfo clientChannelInfo = JDBCTransactionStore.this.brokerController.getProducerManager()
				.pickProducerChannelRandomly(producerGroupHashCode);
		if (null == clientChannelInfo) {
			log.warn("check a producer transaction state, but not find any channel of this group[{}]",
					producerGroupHashCode);
			return;
		}

		// 第二步、查询消息
		SelectMapedBufferResult selectMapedBufferResult = JDBCTransactionStore.this.brokerController.getMessageStore()
				.selectOneMessageByOffset(commitLogOffset, msgSize);
		if (null == selectMapedBufferResult) {
			log.warn("check a producer transaction state, but not find message by commitLogOffset: {}, msgSize: {}",
					commitLogOffset, msgSize);
			return;
		}

		// ByteBuffer bb = selectMapedBufferResult.getByteBuffer();

		// 第三步、向Producer发起请求
		final CheckTransactionStateRequestHeader requestHeader = new CheckTransactionStateRequestHeader();
		requestHeader.setCommitLogOffset(commitLogOffset);
		requestHeader.setTranStateTableOffset(tranStateTableOffset);
		JDBCTransactionStore.this.brokerController.getBroker2Client()
				.checkProducerTransactionState(clientChannelInfo.getChannel(), requestHeader, selectMapedBufferResult);
	}
}
