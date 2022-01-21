package DeptWiseSalaryDistribution;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class DeptWiseSalaryDriver {

		public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
			Configuration conf=new Configuration();
			String[] Otherargs=new GenericOptionsParser(conf,args).getRemainingArgs();
			
			Job job=new Job(conf,"No of emp left");
			job.setJarByClass(DeptWiseSalaryDriver.class);
			job.setMapperClass(DeptWiseSalaryMapper.class);
			job.setReducerClass(DeptWiseSalaryReducer.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			FileInputFormat.addInputPath(job, new Path(Otherargs[0]));
			FileOutputFormat.setOutputPath(job, new Path(Otherargs[1]));
			System.exit(job.waitForCompletion(true)?0:1);
			
			
		}
		// TODO Auto-generated method stub

	

}
