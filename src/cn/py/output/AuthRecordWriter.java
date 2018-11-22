package cn.py.output;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class AuthRecordWriter<K, V> extends RecordWriter<K, V>{
	
	private FSDataOutputStream out;
	
	public AuthRecordWriter(FSDataOutputStream out) {
		this.out = out;
	}

	@Override
	public void write(K key, V value) throws IOException, InterruptedException {
		//�����keyѧ���ļ�����֮��mapper���������Mapper�����key
		//�������mapper��reduce,����reduce�����key
		out.write(key.toString().getBytes());
		//���kv�ָ�����Ĭ����Tab�Ʊ��
		out.write("|".getBytes());
		//�������valueд���ļ���
		out.write(value.toString().getBytes());
		//���������֮��ķָ���
		out.write("@@@".getBytes());
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {
		out.close();
	}
	
}
