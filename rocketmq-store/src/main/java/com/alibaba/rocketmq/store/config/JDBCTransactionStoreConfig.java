package com.alibaba.rocketmq.store.config;

class JDBCTransactionStoreConfig {
    private String jdbcDriverClass = "com.mysql.jdbc.Driver";
    private String jdbcURL = "jdbc:mysql://xxx.xxx.xxx.xxx:1000/xxx?useUnicode=true&characterEncoding=UTF-8";
    private String jdbcUser = "xxx";
    private String jdbcPassword = "xxx";
    // 事务回查至少间隔时间
    private int checkTransactionMessageAtleastInterval = 1000 * 60;
    // 事务回查定时间隔时间
    private int checkTransactionMessageTimerInterval = 1000 * 60;
    // 是否开启事务Check过程，双十一时，可以关闭
    private boolean checkTransactionMessageEnable = true;

    public String getJdbcDriverClass() {
        return jdbcDriverClass;
    }


    public void setJdbcDriverClass(String jdbcDriverClass) {
        this.jdbcDriverClass = jdbcDriverClass;
    }


    public String getJdbcURL() {
        return jdbcURL;
    }


    public void setJdbcURL(String jdbcURL) {
        this.jdbcURL = jdbcURL;
    }


    public String getJdbcUser() {
        return jdbcUser;
    }


    public void setJdbcUser(String jdbcUser) {
        this.jdbcUser = jdbcUser;
    }


    public String getJdbcPassword() {
        return jdbcPassword;
    }


    public void setJdbcPassword(String jdbcPassword) {
        this.jdbcPassword = jdbcPassword;
    }

    public int getCheckTransactionMessageAtleastInterval() {
        return checkTransactionMessageAtleastInterval;
    }

    public void setCheckTransactionMessageAtleastInterval(int checkTransactionMessageAtleastInterval) {
        this.checkTransactionMessageAtleastInterval = checkTransactionMessageAtleastInterval;
    }

    public int getCheckTransactionMessageTimerInterval() {
        return checkTransactionMessageTimerInterval;
    }

    public void setCheckTransactionMessageTimerInterval(int checkTransactionMessageTimerInterval) {
        this.checkTransactionMessageTimerInterval = checkTransactionMessageTimerInterval;
    }
}
