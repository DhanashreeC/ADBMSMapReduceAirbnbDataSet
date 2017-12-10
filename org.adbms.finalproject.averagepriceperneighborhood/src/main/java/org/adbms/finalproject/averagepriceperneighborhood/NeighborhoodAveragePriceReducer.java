package org.adbms.finalproject.averagepriceperneighborhood;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class NeighborhoodAveragePriceReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

	private FloatWritable average = new FloatWritable();

	public void reduce(Text key, Iterable<FloatWritable> values, Context context)
			throws IOException, InterruptedException {
		float sum = 0;
		int count = 0;
		for (FloatWritable val : values) {
			sum += val.get();
			count++;
		}

		average.set(sum / (float) count);
		context.write(key, average);
	}
}
