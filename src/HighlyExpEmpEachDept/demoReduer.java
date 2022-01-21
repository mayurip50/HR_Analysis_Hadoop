package HighlyExpEmpEachDept;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class demoReduer extends Reducer<Text,IntWritable, Text,DoubleWritable>{
	private final DoubleWritable result=new DoubleWritable();
	public void reduce(Text key,Iterable<IntWritable> values,Context context ) throws IOException, InterruptedException {

	int max=0;
	//double count=0.0;

	for(IntWritable val : values) {
		if(max<val.get())
		{
			max=val.get();
		}

	}
	
	result.set(max);
	context.write(key, result);

	
	}
		
	}
