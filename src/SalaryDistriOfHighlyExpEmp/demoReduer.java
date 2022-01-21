package SalaryDistriOfHighlyExpEmp;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class demoReduer extends Reducer<Text,MultipleVariableWritable, Text,Text>{
	private final DoubleWritable result=new DoubleWritable();
	public void reduce(Text key,Iterable<MultipleVariableWritable> values,Context context ) throws IOException, InterruptedException {

	int max=0;
	//double count=0.0;
String salary="";
	for(MultipleVariableWritable val : values) {
		if(max<val.val1)
		{
			max=val.val1;
			salary=val.val2;
		}

	}
	
	//result.set(max);
	context.write(key, new Text(max+" "+salary));

	
	}
		
	}
