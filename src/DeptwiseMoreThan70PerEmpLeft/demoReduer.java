package DeptwiseMoreThan70PerEmpLeft;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class demoReduer extends Reducer<Text,IntWritable, Text,DoubleWritable>{
	private final DoubleWritable result=new DoubleWritable();
	public void reduce(Text key,Iterable<IntWritable> values,Context context ) throws IOException, InterruptedException {
		int sum=0;
		double count=0.0;

		for(IntWritable val : values) {
			sum+=val.get();
			count++;
		}
	Integer per=(int) ((sum*100)/count);
	if(per>70) {
		result.set(per);
		context.write(key, result);
	}
	}
		
	}
