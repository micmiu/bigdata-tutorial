package com.micmiu.hive.fotmater;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

/**
 * 
 * hive 自定义分隔符 比如：|@^_^@|
 * http://www.micmiu.com/bigdata/hive/hive-inputformat-string/
 * 
 * @author <a href="http://www.micmiu.com">Michael</a>
 * @create Feb 24, 2014 3:11:16 PM
 * @version 1.0
 */
public class MyDemoInputFormat extends TextInputFormat {

	@Override
	public RecordReader<LongWritable, Text> getRecordReader(
			InputSplit genericSplit, JobConf job, Reporter reporter)
			throws IOException {
		reporter.setStatus(genericSplit.toString());
		MyDemoRecordReader reader = new MyDemoRecordReader(
				new LineRecordReader(job, (FileSplit) genericSplit));
		return reader;
	}

	public static class MyDemoRecordReader implements
			RecordReader<LongWritable, Text> {

		LineRecordReader reader;
		Text text;

		public MyDemoRecordReader(LineRecordReader reader) {
			this.reader = reader;
			text = reader.createValue();
		}

		@Override
		public void close() throws IOException {
			reader.close();
		}

		@Override
		public LongWritable createKey() {
			return reader.createKey();
		}

		@Override
		public Text createValue() {
			return new Text();
		}

		@Override
		public long getPos() throws IOException {
			return reader.getPos();
		}

		@Override
		public float getProgress() throws IOException {
			return reader.getProgress();
		}

		@Override
		public boolean next(LongWritable key, Text value) throws IOException {
			while (reader.next(key, text)) {
				// michael|@^_^@|hadoop|@^_^@|http://www.micmiu.com/opensource/hadoop/hive-metastore-config/
				String strReplace = text.toString().toLowerCase()
						.replaceAll("\\|@\\^_\\^@\\|", "\001");
				Text txtReplace = new Text();
				txtReplace.set(strReplace);
				value.set(txtReplace.getBytes(), 0, txtReplace.getLength());
				return true;

			}
			return false;
		}
	}
}
