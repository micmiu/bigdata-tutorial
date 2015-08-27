package com.micmiu.bigdata.hbase.test;

/**
 * Created
 * User: <a href="http://micmiu.com">micmiu</a>
 * Date: 8/27/2015
 * Time: 14:12
 */

import static org.junit.Assert.*;

import com.micmiu.bigdata.hbase.client.HBaseConnPoolManager;
import org.apache.hadoop.hbase.client.HConnection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class HBaseConnPoolManagerTest {

	private HBaseConnPoolManager manager;

	@Before
	public void setUp() {
		String quorum = "192.168.0.30,192.168.0.31,192.168.0.32";
		//quorum = "192.168.8.191,192.168.1.192,192.168.1.193";
		int port = 2181;
		String znode = "/hyperbase1"; //hbase
		manager = new HBaseConnPoolManager(quorum, port, znode);
	}

	@After
	public void tearDown() {
		manager.closeConn();
	}

	@Test
	public void testConn() {
		HConnection coon = manager.getConn();
		assertNotNull(coon);
		assertEquals(coon, manager.getConn());
	}
}
