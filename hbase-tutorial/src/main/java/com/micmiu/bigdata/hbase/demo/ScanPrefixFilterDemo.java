package com.micmiu.bigdata.hbase.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * ScanPrefixFilter
 * User: <a href="http://micmiu.com">micmiu</a>
 * Date: 8/20/2015
 * Time: 12:19
 */
public class ScanPrefixFilterDemo {

	public static Configuration configuration;
	public static HConnection hconn;

	static {
		if (configuration == null)
			configuration = HBaseConfiguration.create();
		if (hconn == null) {
			try {
				hconn = HConnectionManager.createConnection(configuration);
			} catch (ZooKeeperConnectionException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			hconn.close();
			System.err.println("parsm error, Usage: tablename rowkeyPrefix ");
			System.exit(-1);
		}
		scanPrefixFilter(args[0], args[1]);
		hconn.close();

	}

	public static void scanPrefixFilter(String tableName, String rowkeyPrefix) {
		HTableInterface table = null;
		ResultScanner rs = null;
		try {
			table = hconn.getTable(tableName);
			List<Map<String, String>> mapLst = new ArrayList<Map<String, String>>();
			Scan s = new Scan();
			s.setFilter(new PrefixFilter(Bytes.toBytes(rowkeyPrefix)));
			rs = table.getScanner(s);

			for (Result r : rs) {
				System.out.print(">>>> cell rowkey= [" + new String(r.getRow()) + "]");
				for (Cell cell : r.rawCells()) {
					System.out.print(">>>> cell rowkey= " + new String(CellUtil.cloneRow(cell)));
					System.out.print(",family= " + new String(CellUtil.cloneFamily(cell)) + ":" + new String(CellUtil.cloneQualifier(cell)));
					System.out.println(", value= [" + new String(CellUtil.cloneValue(cell)) + "]");
				}

			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (table != null)
					table.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
