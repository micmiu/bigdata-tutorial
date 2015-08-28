package com.micmiu.bigdata.hbase;


import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;

/**
 * Hbase Table common handler method
 * User: <a href="http://micmiu.com">micmiu</a>
 * Date: 7/7/2015
 * Time: 23:27
 */
public class HBaseUtils {

	public final static String DEF_ENCODING = "UTF-8";

	private static final Logger LOGGER = LoggerFactory.getLogger(HBaseUtils.class);

	public static Charset getCharset(String encoding) {
		return ((null == encoding || "".equals(encoding.trim())) ? Charset.forName(DEF_ENCODING) : Charset.forName(encoding));
	}

	public static void close(HBaseAdmin admin) {
		try {
			if (null != admin) {
				admin.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * put a cell data into a row identified by rowKey,columnFamily,identifier
	 *
	 * @param table
	 * @param rowKey
	 * @param family
	 * @param qualifier
	 * @param data
	 * @throws Exception
	 */
	public static void put(HTableInterface table, String rowKey, String family, String qualifier, String data) throws Exception {
		Put put = new Put(Bytes.toBytes(rowKey));
		put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(data));
		table.put(put);
	}

	/**
	 * get a row identified by rowkey
	 *
	 * @param table
	 * @param rowKey
	 * @throws Exception
	 */
	public static Result get(HTableInterface table, String rowKey) throws Exception {
		Get get = new Get(Bytes.toBytes(rowKey));
		Result result = table.get(get);
		return result;
	}

	/**
	 * delete a row by rowkey
	 *
	 * @param table
	 * @param rowKey
	 * @throws Exception
	 */
	public static void deleteRow(HTableInterface table, String rowKey) throws Exception {
		Delete delete = new Delete(Bytes.toBytes(rowKey));
		table.delete(delete);
		LOGGER.info(">>>> HBase Delete {} row with key = {} ", new String(table.getTableName()), rowKey);
	}

	/**
	 * delete a row column family by rowkey
	 *
	 * @param table
	 * @param rowKey
	 * @throws Exception
	 */
	public static void deleteFamily(HTableInterface table, String rowKey, String family) throws Exception {
		Delete delete = new Delete(Bytes.toBytes(rowKey));
		delete.deleteFamily(Bytes.toBytes(family));
		table.delete(delete);
		LOGGER.info(">>>> HBase Delete {} data with key = {}, columnFamily = {}.", new String(table.getTableName()), rowKey, family);
	}

	/**
	 * delete a row family:qualifier data by rowkey
	 *
	 * @param table
	 * @param rowKey
	 * @throws Exception
	 */
	public static void deleteQualifier(HTableInterface table, String rowKey, String family, String qualifier) throws Exception {
		Delete delete = new Delete(Bytes.toBytes(rowKey));
		delete.deleteColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
		table.delete(delete);
		LOGGER.info(">>>> HBase Delete {} data with key = {}, columnFamily = {}, qualifier = {}.", new String(table.getTableName()), rowKey, family, qualifier);
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


	/**
	 * return all match row from a table by scan filter
	 *
	 * @param table
	 * @throws Exception
	 */
	public static ResultScanner scan(HTableInterface table, Scan s) throws Exception {
		ResultScanner rs = table.getScanner(s);
		return rs;
	}

	/**
	 * print table info
	 *
	 * @param table
	 */
	public static void printTableInfo(HTableInterface table) {
		try {
			HTableDescriptor desc = table.getTableDescriptor();
			LOGGER.info(">>>> Print Table {} Desc", new String(table.getTableName()));
			for (HColumnDescriptor colDesc : desc.getColumnFamilies()) {
				LOGGER.info(">>>> family column: {}", colDesc.getNameAsString());

			}
		} catch (Exception ex) {
			LOGGER.error("printTable info Error:", ex);
		}
	}

	/**
	 * print info for Result
	 *
	 * @param r
	 */
	public static void printResultInfo(Result r) {
		System.out.print(">>>> cell rowkey= [" + new String(r.getRow()) + "]");
		for (Cell cell : r.rawCells()) {
			System.out.print(">>>> cell rowkey= " + new String(CellUtil.cloneRow(cell)));
			System.out.print(",family= " + new String(CellUtil.cloneFamily(cell)) + ":" + new String(CellUtil.cloneQualifier(cell)));
			System.out.println(", value= [" + new String(CellUtil.cloneValue(cell)) + "]");
		}
	}
}
