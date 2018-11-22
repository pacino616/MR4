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

	// --�ļ���Ƭ
	private FileSplit fs;
	// --����key
	private IntWritable key;
	// --����value
	private Text value;
	// --�ж�ȡ��
	private LineReader reader;
	// --���ڼ�¼�к�
	private int count;

	/*
	 * ��ʼ�����������ڳ�ʼ���ļ���Ƭ���ж�ȡ��
	 */
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

	/**
	 * �˷������return true,�ͻ��ٴα����ã� ���return false,�Ͳ��ٵ��ã� ����������У�һ��һ�еĴ����ļ���ֱ��������
	 */
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// --����key�ĳ�ʼ��
		key = new IntWritable();
		// --����value�ĳ�ʼ��
		value = new Text();

		Text tmp = new Text();

		// --readLineÿ����һ�Σ��ͻ��ȡһ������
		int length = reader.readLine(tmp);
		if (length == 0) {
			// --���length=0,˵����ǰ��Ƭ��Ӧ���ļ������Ѷ���
			// --�õ�ǰ����ֹͣ����
			return false;
		} else {
			count++;
			// --���кŸ�ֵ������key
			key.set(count);

			// --��ÿ�����ݸ�ֵ������value
			value.set(tmp);
			
			
			return true;
		}
	}

	/*
	 * �˷������ڽ�����key����Mapper����� nextKeyValue()����һ�Σ��˷���Ҳ�ᱻ����һ��
	 */
	@Override
	public IntWritable getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	/*
	 * �˷������ڽ�����value����Mapper����� nextKeyValue()����һ�Σ��˷���Ҳ�ᱻ����һ��
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
