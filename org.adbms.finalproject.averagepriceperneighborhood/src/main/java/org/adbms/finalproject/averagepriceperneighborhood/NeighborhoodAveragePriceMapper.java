package org.adbms.finalproject.averagepriceperneighborhood;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class NeighborhoodAveragePriceMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
		FloatWritable price = new FloatWritable();
        String[] fields = value.toString().split(",");
        Text neighborhood = new Text(fields[1]);
        if(!neighborhood.toString().contains("neighbourhood")) {
        	price.set(Float.parseFloat(fields[2]));
            context.write(neighborhood, price);
        }
	}
}
