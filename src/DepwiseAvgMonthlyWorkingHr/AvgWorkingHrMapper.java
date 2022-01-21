package DepwiseAvgMonthlyWorkingHr;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class AvgWorkingHrMapper extends Mapper<Object,Text,Text,IntWritable>{
	private final Text department=new Text();
	private final IntWritable workingHr=new IntWritable();
	
	public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
		
		String[] tokens=value.toString().split(",");
		
		String dept=tokens[8].trim();
		department.set(dept);
		
		Integer wr=Integer.parseInt(tokens[3].trim());	
		
		workingHr.set(wr);
		
context.write(department, workingHr);
		
		
	}
}