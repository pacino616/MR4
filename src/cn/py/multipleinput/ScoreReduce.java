package cn.py.multipleinput;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class ScoreReduce extends Reducer<Text, Text, Text, Text> {

	// --获取多输出源，泛型分别对应：输出key和输出value
	private MultipleOutputs<Text, Text> mos;

	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		for (Text value : values) {
			if (key.toString().equals("tom")) {
				// --通过多输出源输出数据,第一个参数是自定义的文件名
				mos.write("tomfile", key, value);
			} else if (key.toString().equals("jim")) {
				mos.write("rosefile", key, value);
			} else {
				mos.write("jimfile", key, value);
			}
		}
	}

	@Override
	protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		// --输出化多输出源
		mos = new MultipleOutputs<>(context);
	}
}
