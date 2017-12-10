package tophosts;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TophostWithMostListingsMapper2 extends Mapper<LongWritable, Text, TopHostCustomWritable, IntWritable>{
	protected void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
		Text hostId = new Text();
		FloatWritable listingCount = new FloatWritable();
		if (values.toString().length() > 0) {
			try {
				String fields[] = values.toString().split("\t");
				hostId.set(fields[0]);
				listingCount.set(Float.parseFloat(fields[1]));
				
				TopHostCustomWritable data = new TopHostCustomWritable(hostId.toString(), listingCount.get());
				
				context.write(data, new IntWritable(1));
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
	}
}
