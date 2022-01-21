package DeptWisePromotedButLeft;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class promotedButLeftReducer extends Reducer<Text,IntWritable, Text,IntWritable>{
	private final IntWritable result=new IntWritable();
	public void reduce(Text key,Iterable<IntWritable> values,Context context ) throws IOException, InterruptedException {
		int sum=0;
		//double count=0.0;

		for(IntWritable val : values) {
			sum+=val.get();
		//	count++;
		}
		
		result.set(sum);
		context.write(key, result);
	}
		
	}
