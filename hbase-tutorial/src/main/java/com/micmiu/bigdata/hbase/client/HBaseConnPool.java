package com.micmiu.bigdata.hbase.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnection;

/**
 * Created
 * User: <a href="http://micmiu.com">micmiu</a>
 * Date: 8/27/2015
 * Time: 10:10
 */
public interface HBaseConnPool {

	void setup();

	Configuration getConfig();

	HConnection getConn();

	void closeConn();
}
