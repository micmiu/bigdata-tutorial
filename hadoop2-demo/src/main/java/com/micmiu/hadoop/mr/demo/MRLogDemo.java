package com.micmiu.hadoop.mr.demo;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * 利用wordcount示例 测试MR中各种日志的输出
 * User: <a href="http://micmiu.com">micmiu</a>
 * Date: 2/14/2015
 * Time: 17:37
 */
public class MRLogDemo {

	private static final Logger LOGGER = LoggerFactory.getLogger(MRLogDemo.class);

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			LOGGER.info(">>>> log mapper value = " + value);
			System.out.println(">>>> out mapper value = " + value);
			System.err.println(">>>> err mapper value = " + value);
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
			LOGGER.info(">>>> log mapper end.");
			System.out.println(">>>> out mapper end.");
			System.err.println(">>>> err mapper end.");
		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			LOGGER.info(">>>> log reduce start ...");
			System.out.println(">>>> out reduce start ...");
			System.err.println(">>>> err reduce start ...");
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
			LOGGER.info(">>>> log reduce end.");
			System.out.println(">>>> out reduce end.");
			System.err.println(">>>> err reduce end.");
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: <in> <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "MRLogDemo");
		job.setJarByClass(MRLogDemo.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
