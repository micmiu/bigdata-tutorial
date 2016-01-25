package com.micmiu.bigdata.hbase.coprocessor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
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
public class RegionObserverDemo extends BaseRegionObserver {
	public static final Log LOG = LogFactory.getLog(RegionObserverDemo.class);
	public static final byte[] FAMILY = Bytes.toBytes("@INFO@");
	public static final byte[] ROW_GETDMEO = Bytes.toBytes("@@@GETDEMO@@@");

	@Override
	public void start(CoprocessorEnvironment e) throws IOException {
		super.start(e);
		LOG.info(" >>>> RegionObserverDemo start ...");
	}

	@Override
	public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<Cell> results) throws IOException {
		LOG.debug("Got preGetOp for row: " + Bytes.toStringBinary(get.getRow()));

		if (Bytes.equals(get.getRow(), ROW_GETDMEO)) {
			KeyValue kv = new KeyValue(get.getRow(), FAMILY, ROW_GETDMEO, Bytes.toBytes("hello,micmiu.com"));
			LOG.debug("coprocess match the row kv: " + kv);
			results.add(kv);
		}
	}

	@Override
	public void stop(CoprocessorEnvironment e) throws IOException {
		super.stop(e);
		LOG.info(" >>>> RegionObserverDemo stop.");
	}
}
