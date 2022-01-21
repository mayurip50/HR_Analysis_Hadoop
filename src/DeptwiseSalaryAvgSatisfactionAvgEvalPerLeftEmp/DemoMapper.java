package DeptwiseSalaryAvgSatisfactionAvgEvalPerLeftEmp;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DemoMapper extends Mapper<Object,Text,Text,MultipleVariableWritable>{
	private final Text department=new Text();
	//private final DoubleWritable satisfaction_level=new DoubleWritable();
	
	public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
		
		String[] tokens=value.toString().split(",");
		
	
		String salary=tokens[9].trim();
	//	Integer promoted=Integer.parseInt(tokens[7].trim());
		//department.set(dept);
		
		String dept=tokens[8].trim();			
		Double sl=Double.parseDouble(tokens[0].trim());	
		Double eval=Double.parseDouble(tokens[1].trim());	
		Integer left=Integer.parseInt(tokens[6].trim());	
		
		MultipleVariableWritable composit_Values=new MultipleVariableWritable(sl,eval,left);
		//satisfaction_level.set(sl);
				context.write(new Text(dept+" "+salary), composit_Values);
		
		
	}

}
