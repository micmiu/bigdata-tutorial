package com.micmiu.hive.fotmater;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.util.LineReader;

/**
 * 
 * @author <a href="http://www.micmiu.com">Michael</a>
 * @create Feb 21, 2014 2:37:05 PM
 * @version 1.0
 */
public class MyDemoRecordReader implements RecordReader<LongWritable, Text> {

	private static final Log LOG = LogFactory.getLog(MyDemoRecordReader.class
			.getName());
	public static final String MAX_LINE_LENGTH = 
		    "mapreduce.input.linerecordreader.line.maxlength";

	private CompressionCodecFactory compressionCodecs = null;
	private long start;
	private long pos;
	private long end;
	private LineReader lineReader;
	int maxLineLength;

	public MyDemoRecordReader(FileSplit inputSplit, Configuration job)
			throws IOException {
		maxLineLength = job.getInt("mapred.micmiu.demo.maxlength",
				Integer.MAX_VALUE);
		start = inputSplit.getStart();
		end = start + inputSplit.getLength();
		final Path file = inputSplit.getPath();
		compressionCodecs = new CompressionCodecFactory(job);
		final CompressionCodec codec = compressionCodecs.getCodec(file);

		// Open file and seek to the start of the split
		FileSystem fs = file.getFileSystem(job);
		FSDataInputStream fileIn = fs.open(file);
		boolean skipFirstLine = false;
		if (codec != null) {
			lineReader = new LineReader(codec.createInputStream(fileIn), job);
			end = Long.MAX_VALUE;
		} else {
			if (start != 0) {
				skipFirstLine = true;
				--start;
				fileIn.seek(start);
			}
			lineReader = new LineReader(fileIn, job);
		}
		if (skipFirstLine) {
			start += lineReader.readLine(new Text(), 0,
					(int) Math.min((long) Integer.MAX_VALUE, end - start));
		}
		this.pos = start;
	}

	public MyDemoRecordReader(InputStream in, long offset, long endOffset,
			int maxLineLength) {
		this.maxLineLength = maxLineLength;
		this.lineReader = new LineReader(in);
		this.start = offset;
		this.pos = offset;
		this.end = endOffset;
	}

	public MyDemoRecordReader(InputStream in, long offset, long endOffset,
			Configuration job) throws IOException {
		this.maxLineLength = job.getInt(
				"mapred.nginxlogrecordreader.maxlength", Integer.MAX_VALUE);
		this.lineReader = new LineReader(in, job);
		this.start = offset;
		this.pos = offset;
		this.end = endOffset;
	}

	public LongWritable createKey() {
		return new LongWritable();
	}

	public Text createValue() {
		return new Text();
	}

	/** Read a line. */
	public synchronized boolean next(LongWritable key, Text value)
			throws IOException {
		while (pos < end) {
			key.set(pos);

			int newSize = lineReader.readLine(value, maxLineLength,
					Math.max((int) Math.min(Integer.MAX_VALUE, end - pos),
							maxLineLength));

			// start
			String strReplace = value.toString().toLowerCase()
					.replaceAll("\\|\\|\\|", "\001");
			Text txtReplace = new Text();
			txtReplace.set(strReplace);
			value.set(txtReplace.getBytes(), 0, txtReplace.getLength());
			// end

			if (newSize == 0) {
				return false;
			}
			pos += newSize;
			if (newSize < maxLineLength) {
				return true;
			}

			// line too long. try again
			LOG.info("Skipped line of size " + newSize + " at pos "
					+ (pos - newSize));
		}

		return false;
	}

	public float getProgress() {
		if (start == end) {
			return 0.0f;
		} else {
			return Math.min(1.0f, (pos - start) / (float) (end - start));
		}
	}

	public synchronized long getPos() throws IOException {
		return pos;
	}

	public synchronized void close() throws IOException {
		if (lineReader != null)
			lineReader.close();
	}
}