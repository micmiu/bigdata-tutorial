package com.micmiu.bigdata.hbase.coprocessor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * Created coprocessor demo
 * User: <a href="http://micmiu.com">micmiu</a>
 */
public class GetInfoController extends BaseRegionObserver {
	public static final Log LOG = LogFactory.getLog(GetInfoController.class);
	public static final byte[] FAMILY = Bytes.toBytes("@INFO@");
	public static final byte[] ROW_GETTIME = Bytes.toBytes("@@@GETTIME@@@");
	public static final byte[] ROW_GETAUTHOR = Bytes.toBytes("@@@GETAUTHOR@@@");

	@Override
	public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<Cell> results) throws IOException {
		LOG.debug("Got preGet for row: " + Bytes.toStringBinary(get.getRow()));

		if (Bytes.equals(get.getRow(), ROW_GETTIME)) {
			KeyValue kv = new KeyValue(get.getRow(), FAMILY, ROW_GETTIME,
					Bytes.toBytes(System.currentTimeMillis()));
			LOG.debug("coprocess match the row kv: " + kv);
			results.add(kv);
		} else if (Bytes.equals(get.getRow(), ROW_GETAUTHOR)) {
			KeyValue kv = new KeyValue(get.getRow(), FAMILY, ROW_GETAUTHOR,
					Bytes.toBytes("micmiu.com"));
			LOG.debug("coprocess match the row kv: " + kv);
			results.add(kv);
		}
	}
}
