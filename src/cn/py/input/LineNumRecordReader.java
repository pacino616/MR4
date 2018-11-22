package cn.py.input;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

public class LineNumRecordReader extends RecordReader<IntWritable, Text> {

	// --文件切片
	private FileSplit fs;
	// --输入key
	private IntWritable key;
	// --输入value
	private Text value;
	// --行读取器
	private LineReader reader;
	// --用于记录行号
	private int count;

	/*
	 * 初始化方法，用于初始化文件切片和行读取器
	 */
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		// 初始化切片
		fs = (FileSplit) split;
		// 通过切片获取切片路径
		Path path = fs.getPath();
		// 获取job的环境变量参数
		Configuration conf = context.getConfiguration();
		// 获取hdfs文件系统
		FileSystem system = path.getFileSystem(conf);
		// 获取切片文件数据的输入流
		InputStream in = system.open(path);
		// 初始化行读取器
		reader = new LineReader(in);
	}

	/**
	 * 此方法如果return true,就会再次被调用， 如果return false,就不再调用， 在这个方法中，一行一行的处理文件，直到处理完
	 */
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// --输入key的初始化
		key = new IntWritable();
		// --输入value的初始化
		value = new Text();

		Text tmp = new Text();

		// --readLine每调用一次，就会读取一行数据
		int length = reader.readLine(tmp);
		if (length == 0) {
			// --如果length=0,说明当前切片对应的文件数据已读完
			// --让当前方法停止调用
			return false;
		} else {
			count++;
			// --将行号赋值给输入key
			key.set(count);

			// --将每行内容赋值给输入value
			value.set(tmp);
			
			
			return true;
		}
	}

	/*
	 * 此方法用于将输入key传给Mapper组件。 nextKeyValue()调用一次，此方法也会被调用一次
	 */
	@Override
	public IntWritable getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	/*
	 * 此方法用于将输入value传给Mapper组件。 nextKeyValue()调用一次，此方法也会被调用一次
	 */
	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return 0;
	}

	@Override
	public void close() throws IOException {
		reader = null;
	}

}
