package com.kv.custompartitioner;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DeptReducer extends Reducer<Text, Text, Text, Text>{
	
	public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException{
		int max_sal = Integer.MIN_VALUE;
		
		String emp_name = " ";
		String emp_dept = " ";
		String emp_des = " ";
		String emp_id = " ";
		int emp_sal = 0;
		
		for(Text val : values) {
			String [] valTokens = val.toString().split(",");
			emp_sal = Integer.parseInt(valTokens[3]);
			if(emp_sal > max_sal)
			{
				emp_id = valTokens[0];
				emp_name = valTokens[1];
				emp_des = valTokens[2];
				emp_dept =key.toString();
				max_sal = emp_sal;
			}
		}
		context.write(new Text(emp_dept), new Text("id=>"+emp_id+",name=>"+emp_name+",des=>"+emp_des+",sal=>"+max_sal));
	}

}
