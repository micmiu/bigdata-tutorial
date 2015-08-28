package com.micmiu.bigdata.hbase.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hbase  table Handler
 * User: <a href="http://micmiu.com">micmiu</a>
 * Date: 7/7/2015
 * Time: 23:35
 */
public class HBaseSimpleDemo {

	private static final Logger LOGGER = LoggerFactory.getLogger(HBaseSimpleDemo.class);

	private String encoding = "UTF-8";

	private Configuration config;

	private HConnection hconn;

	public HBaseSimpleDemo() {
		this.config = HBaseConfiguration.create();
	}

	public HBaseSimpleDemo(Configuration config) {
		this.config = config;
	}

	public HBaseSimpleDemo(String quorum, int port) {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.property.clientPort", port + "");
		conf.set("hbase.zookeeper.quorum", quorum);
		//默认为 : /hbase  EDH: /hyperbase1
		conf.set("zookeeper.znode.parent", "/hyperbase1");
		this.config = conf;
		this.openConn();

	}

	public HConnection openConn() {
		if (null == hconn) {
			try {
				this.hconn = HConnectionManager.createConnection(config);
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
		return hconn;
	}

	public Boolean createTable(String tableName, String familyName) throws Exception {
		HBaseAdmin admin = new HBaseAdmin(hconn);
		if (admin.tableExists(tableName)) {
			LOGGER.warn(">>>> Table {} exists!", tableName);
			admin.close();
			return false;
		}
		HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
		tableDesc.addFamily(new HColumnDescriptor(familyName));
		admin.createTable(tableDesc);
		LOGGER.info(">>>> Table {} create success!", tableName);

		admin.close();
		return true;

	}

}
