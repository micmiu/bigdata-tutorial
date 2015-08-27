package com.micmiu.bigdata.hbase.test;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * HBase CRUD 基本操作
 * User: <a href="http://micmiu.com">micmiu</a>
 * Date: 6/2/2015
 * Time: 15:58
 */
public class HBaseFactoryTest {


	private static final Logger LOGGER = LoggerFactory.getLogger(HBaseFactoryTest.class);

	private Configuration config;
	private HConnection conn = null;

	public HBaseFactoryTest() {
		this.config = HBaseConfiguration.create();
		this.openConn();
	}

	public HBaseFactoryTest(Configuration config) {
		this.config = config;
		this.openConn();
	}

	public HBaseFactoryTest(String quorum, int port) {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.property.clientPort", port + "");
		conf.set("hbase.zookeeper.quorum", quorum);
		//默认为 : /hbase  EDH: /hyperbase1
//		conf.set("zookeeper.znode.parent", "/hyperbase1");
		//config.set("hbase.master", "192.168.0.30:60000");
		//config.set("hbase.master.port", "60000");
		this.config = conf;
		this.openConn();

	}

	public HConnection openConn() {
		if (null == conn) {
			try {
				this.conn = HConnectionManager.createConnection(config);
			} catch (Exception ex) {
				ex.printStackTrace();
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

	public HTableInterface getTable(String tableName) throws Exception {
		return conn.getTable(tableName.getBytes("utf-8"));
	}

	public void printTableDesc(String tableName) {
		try {
			HTableInterface table = getTable(tableName);
			HTableDescriptor desc = table.getTableDescriptor();
			LOGGER.info(">>>> Print Table {} Desc", tableName);
			for (HColumnDescriptor colDesc : desc.getColumnFamilies()) {
				LOGGER.info(">>>> family column: {}", colDesc.getNameAsString());

			}
		} catch (Exception ex) {
			LOGGER.error(">>>> Print table desc error:", ex);
		}
	}

	public Boolean createTable(String tableName, String familyName) throws Exception {
		HBaseAdmin admin = new HBaseAdmin(conn);
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

	/**
	 * @param tableName
	 * @return
	 */
	public boolean deleteTable(String tableName) throws IOException {

		HBaseAdmin admin = new HBaseAdmin(conn);
		if (admin.tableExists(tableName)) {
			try {
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
				LOGGER.info(">>>> Table {} delete success!", tableName);
			} catch (Exception ex) {
				LOGGER.error("delete table error:", ex);
				return false;
			}
		} else {
			LOGGER.warn(">>>> Table {} delete but not exist.", tableName);
		}
		admin.close();
		return true;
	}

	/**
	 * put a cell data into a row identified by rowKey,columnFamily,identifier
	 *
	 * @param table
	 * @param rowKey
	 * @param columnFamily
	 * @param qualifier
	 * @param data
	 * @throws Exception
	 */
	public static void putCell(HTableInterface table, String rowKey, String columnFamily, String qualifier, String data) throws Exception {
		Put put = new Put(Bytes.toBytes(rowKey));
		put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier), Bytes.toBytes(data));
		table.put(put);
	}

	/**
	 * get a row identified by rowkey
	 *
	 * @param table
	 * @param rowKey
	 * @throws Exception
	 */
	public static Result getRow(HTableInterface table, String rowKey) throws Exception {
		Get get = new Get(Bytes.toBytes(rowKey));
		Result result = table.get(get);
		return result;
	}

	/**
	 * delete a row identified by rowkey
	 *
	 * @param table
	 * @param rowKey
	 * @throws Exception
	 */
	public static void deleteRow(HTableInterface table, String rowKey) throws Exception {
		Delete delete = new Delete(Bytes.toBytes(rowKey));
		table.delete(delete);
		LOGGER.info(">>>> Delete row: " + rowKey);
	}

	/**
	 * return all row from a table
	 *
	 * @param table
	 * @throws Exception
	 */
	public static ResultScanner scanAll(HTableInterface table) throws Exception {
		Scan s = new Scan();
		ResultScanner rs = table.getScanner(s);
		return rs;
	}

	public static void main(String[] args) throws Exception {

		String quorum = "192.168.0.30,192.168.0.31,192.168.0.32";
		quorum = "192.168.8.191,192.168.1.192,192.168.1.193";
		int port = 2181;
		HBaseFactoryTest factory = new HBaseFactoryTest(quorum, port);

		String tableName = "demo_test";
		String columnFamily = "cf";

		System.out.println("=============================== : create");
		factory.createTable(tableName, columnFamily);
		System.out.println("=============================== : print");
		factory.printTableDesc(tableName);
		System.out.println("=============================== : put");
		HTableInterface table = factory.getTable(tableName);
		table.setAutoFlushTo(false);
		for (int i = 0; i < 10; i++) {
			putCell(table, "rowkey" + i, columnFamily, "info", "micmiu-" + i);
		}
		table.flushCommits();
		table.close();

		System.out.println("=============================== : query");
		ResultScanner rs = HBaseFactoryTest.scanAll(table);
		for (Result result : rs) {
			System.out.println(">>>> result Empty : " + result.isEmpty());
			for (Cell cell : result.rawCells()) {
				System.out.print(">>>> cell rowkey= " + new String(CellUtil.cloneRow(cell)));
				System.out.print(",family= " + new String(CellUtil.cloneFamily(cell)) + ":" + new String(CellUtil.cloneQualifier(cell)));
				System.out.println(", value= " + new String(CellUtil.cloneValue(cell)));
			}
		}

		System.out.println("=============================== : delete");
		factory.deleteTable(tableName);

		factory.closeConn();
	}

}
