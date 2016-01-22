package com.micmiu.bigdata.hbase.coprocessor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

/**
 * Created
 * User: <a href="http://micmiu.com">micmiu</a>
 */
public class TableCoprocessorCreateDemo {

	public static void main(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		FileSystem fs = FileSystem.get(conf);
		//coprocessor所在的jar包的存放路径
		Path path = new Path(fs.getUri() + Path.SEPARATOR + "micmiu/coprocessor/demo.jar");
		//HTableDescriptor
		HTableDescriptor htd = new HTableDescriptor("demo_copro");
		//addFamily
		htd.addFamily(new HColumnDescriptor("cf"));
		//
		//设置要加载的corpocessor
		htd.setValue("COPROCESSOR$1", path.toString() +
				"|" + RegionObserverDemo.class.getCanonicalName() +
				"|" + Coprocessor.PRIORITY_USER);
		//
		HBaseAdmin admin = new HBaseAdmin(conf);

		//创建表"testtable"
		admin.createTable(htd);

		System.out.println("finished.");
	}
}
