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
		//将输出key学到文件里，如果之后mapper组件，则是Mapper的输出key
		//如果既有mapper和reduce,则是reduce的输出key
		out.write(key.toString().getBytes());
		//输出kv分隔符，默认是Tab制表符
		out.write("|".getBytes());
		//将输出的value写入文件中
		out.write(value.toString().getBytes());
		//输出行与行之间的分隔符
		out.write("@@@".getBytes());
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {
		out.close();
	}
	
}
