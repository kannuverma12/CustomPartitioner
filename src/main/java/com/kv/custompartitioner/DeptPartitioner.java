package com.kv.custompartitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class DeptPartitioner extends Partitioner<Text, Text>{

	@Override
	public int getPartition(Text key, Text value, int numPartitions) {

		String empDept = key.toString();
		
		if(numPartitions == 0)
			return 0;
		if(empDept.equals("Program Department"))
			return 1 % numPartitions;
		else if(empDept.equals("Admin Department"))
			return 2 % numPartitions;
		else
			return 3 % numPartitions;
			
	}

}
