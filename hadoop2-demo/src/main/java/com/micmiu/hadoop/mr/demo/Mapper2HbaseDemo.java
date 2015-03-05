package com.micmiu.hadoop.mr.demo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;


/**
 * 利用MR的 mapper执行数据分析入库到hbase，reduce task 数量设置为0
 * User: <a href="http://micmiu.com">micmiu</a>
 * Date: 3/4/2015
 * Time: 12:46
 */
public class Mapper2HbaseDemo extends Configured implements Tool {

	private static final Log LOG = LogFactory.getLog(Mapper2HbaseDemo.class);

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		int res = 1;
		try {
			res = ToolRunner.run(conf, new Mapper2HbaseDemo(), otherArgs);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(res);

	}

	public static class Map extends Mapper<LongWritable, Text, NullWritable, NullWritable> {
		private Configuration conf = null;
		private HTable htable = null;
		private boolean wal = true;
		private Put put = null;
		static long count = 0;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			conf = HBaseConfiguration.create(context.getConfiguration());
			conf.set("hbase.zookeeper.quorum", "zk1.hadoop,zk2.hadoop,zk3.hadoop");
			conf.set("hbase.zookeeper.property.clientPort", "2181");

			htable = new HTable(conf, "micmiu");
			htable.setAutoFlush(false);
			htable.setWriteBufferSize(12 * 1024 * 1024);//12M
			wal = true;
		}

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String arr[] = value.toString().split(",");
			if (arr.length == 2) {
				put = new Put(Bytes.toBytes(arr[0]));
				put.add(Bytes.toBytes("blog"), Bytes.toBytes("url"),
						Bytes.toBytes(arr[1]));
				htable.put(put);
				if ((++count % 100) == 0) {
					context.setStatus("Mapper has insert records=" + count);
					context.progress();
					LOG.info("Mapper has insert records=" + count);
				}
			}

			if (!wal) {
				put.setWriteToWAL(false);
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			super.cleanup(context);
			htable.flushCommits();
			htable.close();
		}

	}

	public int run(String[] args) throws Exception {
		String input = args[0];
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, Mapper2HbaseDemo.class.getSimpleName());
		job.setJarByClass(Mapper2HbaseDemo.class);
		job.setMapperClass(Map.class);
		job.setNumReduceTasks(0);
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.setInputPaths(job, input);
		job.setOutputFormatClass(NullOutputFormat.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}


}
