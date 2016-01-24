package com.micmiu.bigdata.hbase.coprocessor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
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
 * Created coprocessor demo for get enviroment
 * User: <a href="http://micmiu.com">micmiu</a>
 */
public class EnvViewer extends BaseRegionObserver {
	public static final Log LOG = LogFactory.getLog(EnvViewer.class);

	public static final byte[] FAMILY = Bytes.toBytes("@INFO@");

	public static final byte[] ROW_GETCONF = Bytes.toBytes("@@@GETCONF@@@");
	public static final byte[] ROW_GETREGION = Bytes.toBytes("@@@GETREGION@@@");

	@Override
	public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<Cell> results) throws IOException {
		LOG.debug("Got preGet for evn key: " + Bytes.toStringBinary(get.getRow()));

		if (Bytes.equals(get.getRow(), ROW_GETCONF)) {
			Configuration conf = e.getEnvironment().getConfiguration();
			KeyValue kv = new KeyValue(get.getRow(), FAMILY, Bytes.toBytes("defaultFS"), Bytes.toBytes(conf.get("fs.defaultFS")));
			results.add(kv);
			kv = new KeyValue(get.getRow(), FAMILY, Bytes.toBytes("nameservices"), Bytes.toBytes(conf.get("dfs.nameservices")));
			results.add(kv);
		} else if (Bytes.equals(get.getRow(), ROW_GETREGION)) {
			KeyValue kv = new KeyValue(get.getRow(), FAMILY, get.getRow(),
					Bytes.toBytes(e.getEnvironment().getRegion().getRegionNameAsString()));
			LOG.debug("coprocess match the row kv: " + kv);
			results.add(kv);
		}
	}
}
