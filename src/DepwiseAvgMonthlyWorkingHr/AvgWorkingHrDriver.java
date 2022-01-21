package DepwiseAvgMonthlyWorkingHr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class AvgWorkingHrDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf=new Configuration();
		String[] Otherargs=new GenericOptionsParser(conf,args).getRemainingArgs();
		
		Job job=new Job(conf,"avg satisfaction level");
		job.setJarByClass(AvgWorkingHrDriver.class);
		job.setMapperClass(AvgWorkingHrMapper.class);
		job.setReducerClass(AvgWorkingHrReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(Otherargs[0]));
		FileOutputFormat.setOutputPath(job, new Path(Otherargs[1]));
		System.exit(job.waitForCompletion(true)?0:1);
		
		
	}
	// TODO Auto-generated method stub



}
