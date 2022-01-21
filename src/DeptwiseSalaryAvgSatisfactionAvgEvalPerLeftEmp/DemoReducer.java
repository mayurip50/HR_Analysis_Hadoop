package DeptwiseSalaryAvgSatisfactionAvgEvalPerLeftEmp;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DemoReducer extends Reducer<Text,MultipleVariableWritable, Text,MultipleVariableWritable>{
//private final DoubleWritable result=new DoubleWritable();
public void reduce(Text key,Iterable<MultipleVariableWritable> values,Context context ) throws IOException, InterruptedException {

	MultipleVariableWritable composite_values=new MultipleVariableWritable ();

	Double count=0.0;

	for(MultipleVariableWritable val : values) {
		//composite_values=val.merge(val);
		composite_values.val1+=val.val1;
		composite_values.val2+=val.val2;
		composite_values.val3+=val.val3;
		count++;
	}
	
	composite_values.val1=composite_values.val1/count;
	composite_values.val2=composite_values.val2/count;
	composite_values.val3=(int) ((composite_values.val3*100)/count);
	
	context.write(key, composite_values);
}
	
}
