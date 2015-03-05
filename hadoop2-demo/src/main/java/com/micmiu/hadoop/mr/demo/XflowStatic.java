package com.micmiu.hadoop.mr.demo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * netflow 各种字段 分析统计工具
 * hadoop jar hadoop2-demo.jar xflowstatic DstAddr /user/micmiu/xflow/in  /user/micmiu/xflow/out_dstaddr
 * @author <a href="http://www.micmiu.com">Michael</a>
 * @create Jan 20, 2014 11:32:16 AM
 * @version 1.0
 * @logs <table cellPadding="1" cellSpacing="1" width="300">
 *       <thead style="font-weight:bold;background-color:#2FABE9">
 *       <tr>
 *       <td>Date</td>
 *       <td>Author</td>
 *       <td>Version</td>
 *       <td>Comments</td>
 *       </tr>
 *       </thead> <tbody style="background-color:#b5cfd2">
 *       <tr>
 *       <td>Jan 20, 2014</td>
 *       <td><a href="http://www.micmiu.com">Michael</a></td>
 *       <td>1.0</td>
 *       <td>Create</td>
 *       </tr>
 *       </tbody>
 *       </table>
 */
public class XflowStatic {

	//SrcAddr DstAddr SrcPort DstPort Protocol
	private static final String TYPE_KEY = "xflow.type";

	public static class XflowMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text ret = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String strLine = value.toString().trim();
			String prefix = context.getConfiguration().get(TYPE_KEY);
			if (strLine.trim().startsWith(prefix + ":")) {
				String[] arr = strLine.split(" ");
				if (arr.length > 1) {
					ret.set(arr[1]);
					context.write(ret, one);
				}
			}
		}
	}

	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: xflowstatic <type> <in> <out>");
			System.exit(2);
		}
		conf.set(TYPE_KEY, otherArgs[0]);
		Job job = Job.getInstance();
		job.setJobName("xflowstatic");
		job.setJarByClass(XflowStatic.class);
		job.setMapperClass(XflowMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
