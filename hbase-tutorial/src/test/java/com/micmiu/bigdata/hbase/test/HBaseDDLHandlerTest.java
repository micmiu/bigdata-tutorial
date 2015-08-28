package com.micmiu.bigdata.hbase.test;

import com.micmiu.bigdata.hbase.HBaseDDLHandler;
import com.micmiu.bigdata.hbase.HBaseUtils;
import com.micmiu.bigdata.hbase.client.HBaseClientManager;
import com.micmiu.bigdata.hbase.client.HBaseConnPool;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created
 * User: <a href="http://micmiu.com">micmiu</a>
 * Date: 7/8/2015
 * Time: 10:28
 */
public class HBaseDDLHandlerTest {

	public static void main(String[] args) throws Exception {
		String quorum = "192.168.0.30,192.168.0.31,192.168.0.32";
		//quorum = "192.168.8.191,192.168.1.192,192.168.1.193";
		int port = 2181;
		String znode = "/hyperbase1";
		HBaseConnPool connPool = new HBaseClientManager(quorum, port, znode);
		HBaseDDLHandler ddlHandler = new HBaseDDLHandler(connPool);

		String tableName = "demo_test";
		System.out.println("=============================== : delete");
		ddlHandler.deleteTable(tableName);

		String columnFamily = "cf";
		System.out.println("=============================== : create");
		ddlHandler.createTable(tableName, columnFamily, "cf2");

		System.out.println("=============================== : desc");
		HBaseUtils.printTableInfo(ddlHandler.getTable(tableName));
		System.out.println("=============================== : alter");
		HBaseAdmin admin = new HBaseAdmin(connPool.getConn());
		admin.disableTable(tableName);
		HTableInterface htable = ddlHandler.getTable(tableName);
		HTableDescriptor tableDesc = admin.getTableDescriptor(htable.getTableName());
		tableDesc.removeFamily(Bytes.toBytes("cf2"));
		HColumnDescriptor newhcd = new HColumnDescriptor("cf3");
		newhcd.setMaxVersions(2);
		newhcd.setKeepDeletedCells(KeepDeletedCells.TRUE);
		tableDesc.addFamily(newhcd);

		admin.modifyTable(tableName, tableDesc);
		admin.enableTable(tableName);
		admin.close();

		System.out.println("=============================== : desc");
		HBaseUtils.printTableInfo(ddlHandler.getTable(tableName));
		System.out.println("=============================== : delete");
		ddlHandler.deleteTable(tableName);

		connPool.closeConn();
	}
}
