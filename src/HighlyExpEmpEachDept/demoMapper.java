package HighlyExpEmpEachDept;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class demoMapper extends Mapper<Object,Text,Text,IntWritable>{
	private final Text department=new Text();
	private final  String salary=new String();
	private final static IntWritable one=new IntWritable(1);
	public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
		
		String[] tokens=value.toString().split(",");
		
		String dept=tokens[8].trim();
			
		Integer exp=Integer.parseInt(tokens[4].trim());	

context.write(new Text(dept), new IntWritable(exp));
		
		
	}

}
