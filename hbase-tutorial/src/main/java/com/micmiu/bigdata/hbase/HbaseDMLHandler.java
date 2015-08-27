package com.micmiu.bigdata.hbase;

import com.micmiu.bigdata.hbase.client.HBaseConnPool;

/**
 * HTable DML handler
 * User: <a href="http://micmiu.com">micmiu</a>
 * Date: 7/8/2015
 * Time: 08:00
 */
public class HBaseDMLHandler extends HBaseBaseHandler {

	public HBaseDMLHandler(HBaseConnPool connPool) {
		super(connPool);
	}
}
