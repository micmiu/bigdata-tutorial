package com.micmiu.hadoop.mr.demo;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * hadoop jar xx.jar  MainClass  -libjars a.jar,b.jar arg0 arg1 ....
 * libjars 文件是需要放在hadoop当前所在文件系统中，不能是hdfs、s3n/s3或者其他系统
 * 这些第三方jar包 都被加载到当前classloader的子loader中，不是当前classloader中
 * User: <a href="http://micmiu.com">micmiu</a>
 * Date: 2/17/2015
 * Time: 16:20
 */
public class MRUseLibjarsDemo extends Configured implements Tool {

	private static final Logger LOGGER = LoggerFactory.getLogger(MRUseLibjarsDemo.class);

	public static class MyJsonMap extends Mapper<LongWritable, Text, Text, IntWritable> {

		private final IntWritable one = new IntWritable(1);
		private Text name = new Text();


		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			LOGGER.info(">>>> libjars(tmpjars) ... " + context.getConfiguration().get("tmpjars"));
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			//"{\"name\":\"Michael\",\"blog\":\"micmiu.com\"}"
			String line = value.toString();
			LOGGER.info(">>>> log mapper line = " + line);
			try {
				JSONObject jsonObj = JSON.parseObject(line);
				if (jsonObj.containsKey("name")) {
					name.set(jsonObj.getString("name"));
					context.write(name, one);
				}
			} catch (Exception e) {
				LOGGER.error("map error", e);
			}
		}
	}

	public static class MyReduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values,
						   Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			LOGGER.info("reduce log >>>> " + key + " = " + sum);
			context.write(key, new IntWritable(sum));
		}
	}


	public int run(String[] args) throws Exception {
		Configuration conf = getConf();

		Job job = Job.getInstance(conf, MRUseLibjarsDemo.class.getSimpleName());
		job.setJarByClass(MRUseLibjarsDemo.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(MyJsonMap.class);
		job.setReducerClass(MyReduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return (job.waitForCompletion(true) ? 0 : -1);
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new MRUseLibjarsDemo(), args);
		LOGGER.info("MR <MRUseLibjarsDemo> run with toolrunner result = {}", res);
		System.exit(res);
	}
}


