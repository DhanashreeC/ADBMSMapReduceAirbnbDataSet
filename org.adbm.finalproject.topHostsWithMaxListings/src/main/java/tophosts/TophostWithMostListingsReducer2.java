package tophosts;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class TophostWithMostListingsReducer2
		extends Reducer<TopHostCustomWritable, IntWritable, TopHostCustomWritable, NullWritable> {

	public static int count = 1;

	@Override
	protected void reduce(TopHostCustomWritable key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		for (IntWritable value : values) {

			if (count < 26) {
				context.write(key, NullWritable.get());
				count++;
			} else {
				break;
			}
		}
	}

}
