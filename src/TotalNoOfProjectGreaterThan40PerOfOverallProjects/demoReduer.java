package TotalNoOfProjectGreaterThan40PerOfOverallProjects;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class demoReduer extends Reducer<Text,IntWritable, Text,IntWritable>{
	int globalCount=0;
	public void reduce(Text key,Iterable<IntWritable> values,Context context ) throws IOException, InterruptedException {

int count=0;
		int sum=0;
	//double count=0.0;
	for(IntWritable val : values) {
		sum+=val.get();
		//count++;
		globalCount+=sum;
	}
	
	//result.set(max);
	Double total_pro_count=(double) (globalCount*(40.0f/100.0f));
		
		 if(sum>total_pro_count) { 	
			 context.write(new Text(key),new IntWritable(sum));
 }
		 	
	//MultipleVariableWritable composite=new MultipleVariableWritable(sum,total_pro_count);
	//String total=String.valueOf(total_pro_count);

	}
		
	}
