package DepwiseAvgSatisfactionLevel;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class satisfactionLevelMapper extends Mapper<Object,Text,Text,DoubleWritable>{
	private final Text department=new Text();
	private final DoubleWritable satisfaction_level=new DoubleWritable();
	
	public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
		
		String[] tokens=value.toString().split(",");
		
		String dept=tokens[8].trim();
		department.set(dept);
		
		Double sl=Double.parseDouble(tokens[0].trim());	
		
		satisfaction_level.set(sl);
		
context.write(department, satisfaction_level);
		
		
	}

}
