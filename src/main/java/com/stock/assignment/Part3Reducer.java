package com.stock.assignment;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Part3Reducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
	
	private DoubleWritable result = new DoubleWritable();
	
	public void reduce(Text key, Iterable<DoubleWritable> values,Context context) 
			throws IOException, InterruptedException {
		int count = 0;
		double averagePrice = 0;
		double price = 0;
		
		for (DoubleWritable val : values) {
			count ++;
			price += val.get();
//			price += Double.parseDouble( val.toString());
		}
		averagePrice = price / count;
//		result.set(price);
		context.write(key, new DoubleWritable(averagePrice));
	}
	

	
}
