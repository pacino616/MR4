package cn.py.output;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 自定义个数输出组件
 * 此组件决定最后结果文件finalout的格式（map组件输出的格式）
 *
 */
public class AuthOutputFormat<K,V> extends FileOutputFormat<K, V>{

	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
		
		Path path = super.getDefaultWorkFile(job, "");
		Configuration conf = job.getConfiguration();
		FileSystem system = path.getFileSystem(conf);
		
		FSDataOutputStream out = system.create(path);
		
		
		return new AuthRecordWriter<K,V>(out);
	}
	
	
}
