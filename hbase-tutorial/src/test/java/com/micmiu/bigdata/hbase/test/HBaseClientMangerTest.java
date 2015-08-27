package com.micmiu.bigdata.hbase.test;

import com.micmiu.bigdata.hbase.client.HBaseClientManager;

/**
 * Created
 * User: <a href="http://micmiu.com">micmiu</a>
 * Date: 8/27/2015
 * Time: 13:18
 */
public class HBaseClientMangerTest {


	public static void main(String[] args) throws Exception {
		String quorum = "192.168.0.30,192.168.0.31,192.168.0.32";
		//quorum = "192.168.8.191,192.168.1.192,192.168.1.193";
		int port = 2181;
		String znode = "/hyperbase1"; //hbase
		HBaseClientManager clientManager = new HBaseClientManager(quorum, port, znode);

		System.out.println("conn => " + clientManager.getConn());

		clientManager.closeConn();

	}
}
