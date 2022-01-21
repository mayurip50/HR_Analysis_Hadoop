package NoOfProjectByDept;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class projectByDeptMapper extends Mapper<Object,Text,Text,IntWritable>{
	private final Text department=new Text();
	private final IntWritable projectDone=new IntWritable();
	
	public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
		
		String[] tokens=value.toString().split(",");
		
		String dept=tokens[8].trim();
		department.set(dept);
		
		Integer projects=Integer.parseInt(tokens[2].trim());	
		
		projectDone.set(projects);
		
context.write(department, projectDone);
		
		
	}

}
