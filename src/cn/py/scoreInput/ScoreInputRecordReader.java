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
		// ��ʼ����Ƭ
		fs = (FileSplit) split;
		// ͨ����Ƭ��ȡ��Ƭ·��
		Path path = fs.getPath();
		// ��ȡjob�Ļ�����������
		Configuration conf = context.getConfiguration();
		// ��ȡhdfs�ļ�ϵͳ
		FileSystem system = path.getFileSystem(conf);
		// ��ȡ��Ƭ�ļ����ݵ�������
		InputStream in = system.open(path);
		// ��ʼ���ж�ȡ��
		reader = new LineReader(in);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		key = new Text();
		value = new Text();
		Text tmp = new Text();
		// --readLineÿ����һ�Σ��ͻ��ȡһ������
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
