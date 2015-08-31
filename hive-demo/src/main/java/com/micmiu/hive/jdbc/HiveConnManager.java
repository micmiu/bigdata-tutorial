package com.micmiu.hive.jdbc;

import org.apache.hadoop.hive.jdbc.HiveConnection;

import java.sql.Connection;

/**
 * Created
 * User: <a href="http://micmiu.com">micmiu</a>
 * Date: 8/31/2015
 * Time: 15:04
 */
public interface HiveConnManager {

	void setup();

	void close();

	Connection getConnection();
}
