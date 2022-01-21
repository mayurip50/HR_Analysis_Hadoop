package DepwiseAvgSatisfactionLevel;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class satisfactionLevelReducer extends Reducer<Text,DoubleWritable, Text,DoubleWritable>{
	private final DoubleWritable result=new DoubleWritable();
	public void reduce(Text key,Iterable<DoubleWritable> values,Context context ) throws IOException, InterruptedException {
		Double sum=0.0;
		Double count=0.0;
Double avg=0.0;
		for(DoubleWritable val : values) {
			sum+=val.get();
			count++;
		}
		
		avg=sum/count;
		result.set(avg);
		context.write(key, result);
	}
		
	}
