package SalaryLowDeptwiseMeanSatisfactionAvgWHLeftEmp;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class DemoDriver {
public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
	Configuration conf=new Configuration();
	String[] Otherargs=new GenericOptionsParser(conf,args).getRemainingArgs();
	
	Job job=new Job(conf,"Avg saticsfaction");
	job.setJarByClass(DemoDriver.class);
	job.setMapperClass(DemoMapper.class);
	job.setReducerClass(DemoReducer.class);
	
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(MultipleVariableWritable.class);
	
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(MultipleVariableWritable.class);
	
	FileInputFormat.addInputPath(job, new Path(Otherargs[0]));
	FileOutputFormat.setOutputPath(job, new Path(Otherargs[1]));
	System.exit(job.waitForCompletion(true)?0:1);
	
	
}
}
