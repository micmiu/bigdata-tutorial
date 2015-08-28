package com.micmiu.bigdata.hbase;

import com.micmiu.bigdata.hbase.client.HBaseClientManager;
import com.micmiu.bigdata.hbase.client.HBaseConnPool;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * HTable DML handler
 * User: <a href="http://micmiu.com">micmiu</a>
 * Date: 7/8/2015
 * Time: 08:00
 */
public class HBaseDMLHandler extends HBaseBaseHandler {

	private static final Logger LOGGER = LoggerFactory.getLogger(HBaseDMLHandler.class);

	public HBaseDMLHandler(HBaseConnPool connPool) {
		super(connPool);
	}

	/**
	 * htable put data
	 *
	 * @param tableName
	 * @param rowKey
	 * @param family
	 * @param qualifier
	 * @param data
	 * @throws Exception
	 */
	public void put(String tableName, String rowKey, String family, String qualifier, String data) throws Exception {
		HTableInterface htable = getTable(tableName);
		Put put = new Put(Bytes.toBytes(rowKey));
		put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(data));
		htable.put(put);

	}

	/**
	 * htable put data
	 *
	 * @param tableName
	 * @param rowKey
	 * @param family
	 * @param qualifiers
	 * @param datas
	 * @throws Exception
	 */
	public void put(String tableName, String rowKey, String family, String[] qualifiers, String[] datas) throws Exception {
		HTableInterface htable = getTable(tableName);
		Put put = new Put(Bytes.toBytes(rowKey));
		for (int i = 0; i < qualifiers.length; i++) {
			if (null != datas && i < datas.length) {
				put.add(Bytes.toBytes(family), Bytes.toBytes(qualifiers[i]), Bytes.toBytes(datas[i]));
			} else {
				put.add(Bytes.toBytes(family), Bytes.toBytes(qualifiers[i]), null);
			}
		}
		htable.put(put);

	}

	/**
	 * htable put data[]
	 *
	 * @param tableName
	 * @param rowKeys
	 * @param family
	 * @param qualifier
	 * @param datas
	 * @throws Exception
	 */
	public void putList(String tableName, String[] rowKeys, String family, String qualifier, String[] datas) throws Exception {
		HTableInterface htable = getTable(tableName);
		List<Put> putList = new ArrayList<Put>();
		for (int i = 0; i < rowKeys.length; i++) {
			Put put = new Put(Bytes.toBytes(rowKeys[i]));
			if (null != datas && i < datas.length) {
				put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(datas[i]));
			} else {
				put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), null);
			}
			putList.add(put);
		}
		htable.put(putList);

	}

	/**
	 * get a row identified by rowkey
	 *
	 * @param tableName
	 * @param rowKey
	 * @throws Exception
	 */
	public Result get(String tableName, String rowKey) throws Exception {
		HTableInterface htable = getTable(tableName);
		Get get = new Get(Bytes.toBytes(rowKey));
		Result result = htable.get(get);
		return result;
	}

	/**
	 * get a row column family by rowkey
	 *
	 * @param tableName
	 * @param rowKey
	 * @param family
	 * @throws Exception
	 */
	public Result get(String tableName, String rowKey, String family) throws Exception {
		HTableInterface htable = getTable(tableName);
		Get get = new Get(Bytes.toBytes(rowKey));
		get.addFamily(Bytes.toBytes(family));
		Result result = htable.get(get);
		return result;
	}

	/**
	 * get a row column qualifier by rowkey
	 *
	 * @param tableName
	 * @param rowKey
	 * @param family
	 * @param qualifier
	 * @return
	 * @throws Exception
	 */
	public Result get(String tableName, String rowKey, String family, String qualifier) throws Exception {
		HTableInterface htable = getTable(tableName);
		Get get = new Get(Bytes.toBytes(rowKey));
		get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
		Result result = htable.get(get);
		return result;
	}

	/**
	 * delete a row identified by rowkey
	 *
	 * @param tableName
	 * @param rowKey
	 * @throws Exception
	 */
	public void deleteRow(String tableName, String rowKey) throws Exception {
		HTableInterface htable = getTable(tableName);
		Delete delete = new Delete(Bytes.toBytes(rowKey));
		htable.delete(delete);
	}

	/**
	 * delete a row identified by rowkey
	 *
	 * @param tableName
	 * @param rowKey
	 * @throws Exception
	 */
	public void deleteFamily(String tableName, String rowKey, String family) throws Exception {
		HTableInterface htable = getTable(tableName);
		Delete delete = new Delete(Bytes.toBytes(rowKey));
		delete.deleteFamily(Bytes.toBytes(family));
		htable.delete(delete);
	}

	/**
	 * delete a row identified by rowkey
	 *
	 * @param tableName
	 * @param rowKey
	 * @throws Exception
	 */
	public void deleteQualifier(String tableName, String rowKey, String family, String qualifier) throws Exception {
		HTableInterface htable = getTable(tableName);
		Delete delete = new Delete(Bytes.toBytes(rowKey));
		delete.deleteColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
		htable.delete(delete);
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
		//quorum = "192.168.8.191,192.168.1.192,192.168.1.193";
		int port = 2181;
		String znode = "/hyperbase1";
		HBaseConnPool connPool = new HBaseClientManager(quorum, port, znode);
		HBaseDMLHandler handler = new HBaseDMLHandler(connPool);

		String tableName = "demo_test";
		handler.put(tableName, "key001", "cf", "name", "Michael");
		handler.put(tableName, "key001", "cf", "sex", "male");
		handler.put(tableName, "key001", "cf2", "blog", "micmiu.com");
		handler.put(tableName, "key001", "cf2", "github", "github.com/micmiu");

		handler.put(tableName, "key002", "cf", new String[]{"name", "sex"}, new String[]{"test", "female"});
		handler.putList(tableName, new String[]{"key010", "key011"}, "cf", "name", new String[]{"Michael010", "Michael011"});

		HBaseUtils.printResultInfo(handler.get(tableName, "key001"));
		HBaseUtils.printResultInfo(handler.get(tableName, "key001", "cf"));
		HBaseUtils.printResultInfo(handler.get(tableName, "key001", "cf2", "blog"));

		connPool.closeConn();
	}

}
