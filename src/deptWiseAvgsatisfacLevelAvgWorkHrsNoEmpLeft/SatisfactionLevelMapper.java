package deptWiseAvgsatisfacLevelAvgWorkHrsNoEmpLeft;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SatisfactionLevelMapper extends Mapper<Object,Text,Text,MultipleVariableWritable>{
	private final Text department=new Text();
	//private final DoubleWritable satisfaction_level=new DoubleWritable();
	
	public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
		
		String[] tokens=value.toString().split(",");
		
		String dept=tokens[8].trim();
		department.set(dept);
		
		Double sl=Double.parseDouble(tokens[0].trim());	
		Integer left=Integer.parseInt(tokens[6].trim());	
		Double wr=Double.parseDouble(tokens[3].trim());	
		
		MultipleVariableWritable composit_Values=new MultipleVariableWritable(sl,wr,left);
		//satisfaction_level.set(sl);
		
context.write(department, composit_Values);
		
		
	}

}
