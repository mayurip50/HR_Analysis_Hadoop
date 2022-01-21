package DeptWisePromotedButLeft;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class promotedButLeftMapper extends Mapper<Object,Text,Text,IntWritable>{
	private final Text department=new Text();
	private final static IntWritable one=new IntWritable(1);
	
	public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
		
		String[] tokens=value.toString().split(",");
		
		String dept=tokens[8].trim();
		department.set(dept);
		
		Integer left=Integer.parseInt(tokens[6].trim());	
		
		Integer promoted=Integer.parseInt(tokens[7].trim());	
		
		if(left==1&&promoted==1)
		{	
			context.write(department, one);
		}
		
	}

}
