package com.stock.assignment;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class Part3Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	
//	private final static IntWritable one = new IntWritable(1);
//    private Text t = new Text();
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
               

            	try{
            		String[] fields = value.toString().split(",");
	                String stock_name = fields[1];
	                double price = Double.parseDouble(fields[4]);

	                context.write(new Text(stock_name), new DoubleWritable(price));
            	}catch(NumberFormatException ex){
            		
            	}
            }
	
        }
	



