package com.micmiu.hive.jdbc;


import org.apache.commons.dbcp.BasicDataSource;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * DBCP 实现 Hive 连接池
 * User: <a href="http://micmiu.com">micmiu</a>
 * Date: 8/12/2015
 * Time: 10:15
 */
public class HiveConnDbcpManager implements HiveConnManager {

	private static final String CONF_FILE_NAME = "/jdbc.properties";

	private static HiveConnDbcpManager instance; // 唯一实例
	private BasicDataSource bds = null;

	/**
	 * 返回唯一实例.如果是第一次调用此方法,则创建实例
	 *
	 * @return DBConnectionManager 唯一实例
	 **/
	public static synchronized HiveConnDbcpManager getInstance() {
		if (instance == null) {
			instance = new HiveConnDbcpManager();
		}
		return instance;
	}

	/**
	 * 建构函数私有以防止其它对象创建本类实例
	 */
	private HiveConnDbcpManager() {
		setup();
	}


	public void setup() {
		if (bds != null) {
			close();
		}
		InputStream is = getClass().getResourceAsStream(CONF_FILE_NAME);
		Properties dbProps = new Properties();
		try {
			dbProps.load(is);
		} catch (Exception e) {
			System.err.println("不能读取属性文件. " + "请确认在当前CLASSPATH下存在配置文件： " + CONF_FILE_NAME);
			e.printStackTrace();
			return;
		}

		bds = new BasicDataSource();
		bds.setDriverClassName(dbProps.getProperty("hive.jdbc.driver", "org.apache.hadoop.hive.jdbc.HiveDriver"));
		bds.setUrl(dbProps.getProperty("hive.jdbc.url"));
		bds.setUsername(dbProps.getProperty("hive.jdbc.user", ""));
		bds.setPassword(dbProps.getProperty("hive.jdbc.password", ""));
		int maxActive = 0;
		try {
			maxActive = Integer.valueOf(dbProps.getProperty("hive.jdbc.maxActive")).intValue();
		} catch (NumberFormatException e) {
			e.printStackTrace();
		}

		if (maxActive < -1)
			maxActive = -1;
		bds.setMaxActive(maxActive);
		bds.setMaxIdle(1);
		bds.setInitialSize(1);
	}

	public void close() {
		try {
			bds.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void close(Connection conn) {
		try {
			conn.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	/**
	 * 获取连接
	 *
	 * @return
	 */
	public synchronized Connection getConnection() {
		try {
			return bds.getConnection();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}


}
