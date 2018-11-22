package cn.py.input;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cn.py.output.AuthOutputFormat;

public class Driver {
	
	public static void main(String[] args) throws Exception{
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf);
		
		job.setJarByClass(Driver.class);
		job.setMapperClass(LineMapper.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		//�����Զ���ĸ�ʽ��������������������Mapper������key������value
		//�������������趨��Ĭ�ϵ���TextInputFormat
		job.setInputFormatClass(LineNumberInputFormat.class);
		
		//�����Զ���ĸ�ʽ��������
		//��������ã�Ĭ��TextOutFormat,����ĸ�ʽ��kv�ָ�����tab�Ʊ����������֮���ǻ��з���
		job.setOutputFormatClass(AuthOutputFormat.class);
		
		//�������ȫ�ֲ�ʽ��·��д����active״̬��namenode��ַ
		FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.80.72:9000/input"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.80.72:9000/input/result"));
		
		job.waitForCompletion(true);
		
	}
}
