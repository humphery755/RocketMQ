package com.alibaba.rocketmq.broker.transaction.jdbc;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.alibaba.rocketmq.store.transaction.TransactionRecord;
import com.alibaba.rocketmq.store.transaction.TransactionStore;


public class JDBCTransactionStoreTest {

    @Test
    public void test_derby_open() {
        JDBCTransactionStoreConfig config = new JDBCTransactionStoreConfig();
        config.setJdbcDriverClass("org.apache.derby.jdbc.EmbeddedDriver");
        config.setJdbcURL("jdbc:derby:xxx;create=true");
        config.setJdbcUser("xxx");
        config.setJdbcPassword("xxx");
        TransactionStore store = new JDBCTransactionStore(config,null);

        boolean open = store.start(true);
        System.out.println(open);
        Assert.assertTrue(open);
        store.shutdown();
    }


    // @Test
    public void test_mysql_open() {
        JDBCTransactionStoreConfig config = new JDBCTransactionStoreConfig();

        TransactionStore store = new JDBCTransactionStore(config,null);

        boolean open = store.start(true);
        System.out.println(open);
        Assert.assertTrue(open);
        store.shutdown();
    }


    // @Test
    public void test_mysql_put() {
        JDBCTransactionStoreConfig config = new JDBCTransactionStoreConfig();

        TransactionStore store = new JDBCTransactionStore(config,null);

        boolean open = store.start(true);
        System.out.println(open);
        Assert.assertTrue(open);

        long begin = System.currentTimeMillis();
        List<TransactionRecord> trs = new ArrayList<TransactionRecord>();
        for (int i = 0; i < 20; i++) {
            TransactionRecord tr = new TransactionRecord();
            tr.setOffset(i);
            tr.setPgroupHashCode(("PG_" + i).hashCode());
            boolean write = store.put(tr);
        }



        System.out.println("TIME=" + (System.currentTimeMillis() - begin));


        store.start(true);
    }


    // @Test
    public void test_mysql_remove() {
        JDBCTransactionStoreConfig config = new JDBCTransactionStoreConfig();

        TransactionStore store = new JDBCTransactionStore(config,null);

        boolean open = store.start(true);
        System.out.println(open);
        Assert.assertTrue(open);

        List<Long> pks = new ArrayList<Long>();
        store.remove(2L);
        store.remove(4L);
        store.remove(6L);
        store.remove(8L);
        store.remove(11L);


        store.shutdown();
    }
}
