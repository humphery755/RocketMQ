package com.alibaba.rocketmq.broker.transaction.jdbc;

import org.junit.Test;

public class JDBCTransactionStoreTest {

	// @Test
	public void test_derby_open() {

	/*	TransactionStore store = new JDBCTransactionStore(config, null);

		boolean open = store.start(true);
		System.out.println(open);
		Assert.assertTrue(open);
		store.shutdown();*/
	}

	// @Test
	public void test_mysql_open() {

		/*TransactionStore store = new JDBCTransactionStore(config, null);

		boolean open = store.start(true);
		System.out.println(open);
		Assert.assertTrue(open);
		store.shutdown();*/
	}

	@Test
	public void test_preparCommit() {

		/*TransactionStore store = new JDBCTransactionStore(config, null);
		
		 * boolean open = store.start(true); System.out.println(open);
		 * Assert.assertTrue(open);
		 

		long begin = System.currentTimeMillis();
		List<TransactionRecord> trs = new ArrayList<TransactionRecord>();
		Map<Long, DispatchRequest> stateTable = new HashMap();
		Map<Long, DispatchRequest> stateTable1 = new HashMap();
		int count = 2000000;
		int j = 0;
		int i = 0;
		while (true) {
			DispatchRequest req = new DispatchRequest("topic", 0, j, 122, i, System.currentTimeMillis(), 0, "key",0, -1,
					"group");
			j++;
			DispatchRequest req1 = new DispatchRequest("topic", 0, j, 122, i, System.currentTimeMillis(), 0, "key", 0, j-1,
					"group");
			store.preparedTransaction(req);
			store.commitTransaction(req1);
			j++;
			i++;
		}*/

	}

	// @Test
	public void test_mysql_remove() {

		/*TransactionStore store = new JDBCTransactionStore(config, null);

		boolean open = store.start(true);
		System.out.println(open);
		Assert.assertTrue(open);

		store.shutdown();*/
	}
}
