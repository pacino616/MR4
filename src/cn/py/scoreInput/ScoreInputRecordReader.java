package cn.py.scoreInput;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

public class ScoreInputRecordReader extends RecordReader<Text, Text> {

	private FileSplit fs;
	private Text key;
	private Text value;
	private LineReader reader;

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

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		key = new Text();
		value = new Text();
		Text tmp = new Text();
		// --readLine每调用一次，就会读取一行数据
		int length = reader.readLine(tmp);
		if (length == 0) {
			return false;
		} else {
			key.set(tmp);
			for (int i = 0; i < 2; i++) {
				reader.readLine(tmp);
				value.append(tmp.getBytes(), 0, tmp.getLength());
				value.append(" ".getBytes(), 0, 1);
			}
			return true;
		}
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return new Text(value.toString().trim());
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
