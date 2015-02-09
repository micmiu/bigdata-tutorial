package com.micmiu.hadoop.mr.demo;

import java.io.IOException;

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
 * 分析netflow 目的IP的工具类
 * hadoop jar hadoop2-demp.jar com.micmiu.hadoop.mr.demo.XflowDstIPCount  /user/micmiu/xflow/in /user/micmiu/xflow/outdstip
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
public class XflowDstIPCount {

	public static class ParesDstIPMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text ip = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String strLine = value.toString().trim();
			if (strLine.trim().startsWith("DstAddr:")) {
				String[] arr = strLine.split(" ");
				if (arr.length > 1) {
					ip.set(arr[1]);
					context.write(ip, one);
				}
			}
		}
	}

	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		String[] otherArgs = new GenericOptionsParser(args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: xflowdstipcount <in> <out>");
			System.exit(2);
		}
		Job job = Job.getInstance();
		job.setJobName("xflow dstip count");
		job.setJarByClass(XflowDstIPCount.class);
		job.setMapperClass(ParesDstIPMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
