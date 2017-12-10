package tophosts;

import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TophostWithMostListingsMapper1 extends Mapper<LongWritable, Text, Text, FloatWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		Text hostId = new Text();
		FloatWritable listingCount = new FloatWritable(1);
		String fields[] = value.toString().split(",");

		if (!fields[1].equals("host_id")) {
			hostId.set(fields[1]);
			context.write(hostId, listingCount);
		}

	}
}
