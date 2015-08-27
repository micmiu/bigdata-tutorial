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
 * Date: 7/8/2015
 * Time: 08:17
 */
public class HBaseClientManager implements HBaseConnPool {

	private static final Logger LOGGER = LoggerFactory.getLogger(HBaseClientManager.class);

	private static final String DEF_ZNODE = "/hbase";

	private Configuration config;
	private HConnection conn;

	public HBaseClientManager() {
		this.config = HBaseConfiguration.create();
	}

	public HBaseClientManager(Configuration config) {
		this.config = config;
	}

	public HBaseClientManager(String quorum, int port) {
		//默认为 : /hbase  EDH: /hyperbase1
		this(quorum, port, DEF_ZNODE);
	}


	public HBaseClientManager(String quorum, int port, String znode) {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.property.clientPort", port + "");
		conf.set("hbase.zookeeper.quorum", quorum);
		conf.set("zookeeper.znode.parent", znode);
		this.config = conf;
	}

	public synchronized HConnection getConn() {
		if (null == conn) {
			try {
				this.conn = HConnectionManager.createConnection(config);
			} catch (Exception ex) {
				LOGGER.error("create conn err:", ex);
			}
		}
		return conn;
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

	public void reloadConfig() {
		this.closeConn();
		this.getConn();
	}

	public void setup() {

	}

	public Configuration getConfig() {
		return config;
	}

	public void setConfig(Configuration config) {
		this.config = config;
		this.reloadConfig();
	}
}
