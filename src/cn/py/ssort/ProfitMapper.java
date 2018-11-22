package cn.py.ssort;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProfitMapper extends Mapper<LongWritable, Text, Profit, NullWritable>{
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Profit, NullWritable>.Context context)
			throws IOException, InterruptedException {
		
		String line=value.toString();
		Profit p=new Profit();
		p.setMonth(Integer.parseInt(line.split(" ")[0]));
		p.setName(line.split(" ")[1]);
		p.setFee(Integer.parseInt(line.split(" ")[2]));
		
		context.write(p, NullWritable.get());
	}
}
