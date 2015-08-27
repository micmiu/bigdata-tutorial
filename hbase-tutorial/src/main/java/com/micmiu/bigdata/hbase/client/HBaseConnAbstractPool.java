package com.micmiu.bigdata.hbase.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created
 * User: <a href="http://micmiu.com">micmiu</a>
 * Date: 8/27/2015
 * Time: 10:32
 */
public abstract class HBaseConnAbstractPool implements HBaseConnPool {

	public static final String DEF_ZNODE = "/hbase";

	private static final Logger LOGGER = LoggerFactory.getLogger(HBaseConnAbstractPool.class);
	private Configuration config;
	private HConnection conn;

	public HBaseConnAbstractPool() {
		this.config = HBaseConfiguration.create();
	}

	public HBaseConnAbstractPool(Configuration config) {
		this.config = config;
	}


	public Configuration getConfig() {
		return config;
	}

	public void closeConn() {
		if (null != conn) {
			try {
				conn.close();
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
	}

	public void setup() {
	}

	public synchronized HConnection getConn() {
		if (null == conn) {
			try {
				this.conn = HConnectionManager.createConnection(this.config);
			} catch (Exception ex) {
				LOGGER.error("create conn err:", ex);
			}
		}
		return conn;
	}
}
