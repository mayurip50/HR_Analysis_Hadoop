package DepwiseEmpLeft;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class EmpLeftMapper extends Mapper<Object,Text,Text,IntWritable>{
	private final Text department=new Text();
	private final IntWritable emp_left=new IntWritable();
	
	public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
		
		String[] tokens=value.toString().split(",");
		
		String dept=tokens[8].trim();
		department.set(dept);
		
		int left=Integer.parseInt(tokens[6].trim());	
		
		emp_left.set(left);
		
context.write(department, emp_left);
		
		
	}

}
