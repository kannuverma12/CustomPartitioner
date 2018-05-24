package com.kv.custompartitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DeptJob extends Configured implements Tool{
	
	 public static void main(String[] args) throws Exception
	 {
		 int res = ToolRunner.run(new Configuration(), new DeptJob(), args);
		 System.exit(res);
	 }

	public int run(String[] args) throws Exception {
		String arg[] = {"/Users/karan.verma/Documents/backups/h/hadoop/input/DeptPartitionerJob",
						"/Users/karan.verma/Documents/backups/h/hadoop/output/DeptPartitionerJob"}; 

		Configuration conf = getConf();
		
		Job deptJob = new Job(conf, "Max Salary");
		deptJob.setJarByClass(DeptJob.class);
		FileInputFormat.setInputPaths(deptJob, new Path(arg[0]));
		FileOutputFormat.setOutputPath(deptJob,new Path(arg[1]));
		deptJob.setMapperClass(DeptMapper.class);
		deptJob.setMapOutputKeyClass(Text.class);
		deptJob.setMapOutputValueClass(Text.class);
		deptJob.setPartitionerClass(DeptPartitioner.class);
		deptJob.setReducerClass(DeptReducer.class);
		deptJob.setNumReduceTasks(3);
		deptJob.setOutputKeyClass(Text.class);
		deptJob.setOutputValueClass(Text.class);
		System.exit(deptJob.waitForCompletion(true)? 0 : 1);
		
		return 0;

	}

}
