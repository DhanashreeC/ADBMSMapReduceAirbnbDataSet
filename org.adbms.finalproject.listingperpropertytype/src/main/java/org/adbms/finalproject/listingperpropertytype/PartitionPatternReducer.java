package org.adbms.finalproject.listingperpropertytype;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PartitionPatternReducer extends Reducer<IntWritable, Text, Text, NullWritable> {

	protected void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		for (Text value : values) {
			context.write(value, NullWritable.get());
		}
	}
}
