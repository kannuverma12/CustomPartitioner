package com.kv.custompartitioner;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DeptMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		String tokens[] = value.toString().split(",");
		String emp_dept = tokens[2].toString();

		String emp_id_n_ds_sal = tokens[0]+","+tokens[1]+","+tokens[3]+","+tokens[4]+","+tokens[5];

		context.write(new Text(emp_dept), new Text(emp_id_n_ds_sal));
	}

}
