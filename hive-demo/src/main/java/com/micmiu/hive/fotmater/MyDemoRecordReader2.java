package com.micmiu.hive.fotmater;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.LineRecordReader;

/**
 * 
 * @author <a href="http://www.micmiu.com">Michael</a>
 * @create Feb 21, 2014 2:37:05 PM
 * @version 1.0
 */
public class MyDemoRecordReader2 extends LineRecordReader{

	public MyDemoRecordReader2(Configuration job, FileSplit split)
			throws IOException {
		super(job, split);
	}

	@Override
	public synchronized boolean next(LongWritable key, Text value)
			throws IOException {
		// TODO Auto-generated method stub
		return super.next(key, value);
	}
	
}