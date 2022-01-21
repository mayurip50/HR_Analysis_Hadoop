package TotalNoOfProjectGreaterThan40PerOfOverallProjects;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class demoMapper extends Mapper<Object,Text,Text,IntWritable>{
	
	public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
		
		String[] tokens=value.toString().split(",");
		
		String dept=tokens[8].trim();
		//String projects=tokens[2].trim();
			
		Integer projects=Integer.parseInt(tokens[2].trim());	
//MultipleVariableWritable composite=new MultipleVariableWritable(exp,salary);
context.write(new Text(dept), new IntWritable(projects));
		
		
	}

}
