package org.adbms.finalproject.cancellationpolicybinning;
import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class BinningMapper extends Mapper<Object, Text, Text, NullWritable> {
	private MultipleOutputs<Text, NullWritable> mos = null;

	protected void setup(Context context) {
		// Create a new MultipleOutputs using the context object
		mos = new MultipleOutputs(context);
	}

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] row = value.toString().split(",");
		String cancellationPolicy = row[1];
		if(cancellationPolicy.equals("cancellation_policy")) {
			return;
		}

		if (cancellationPolicy.equalsIgnoreCase("moderate")) {
			mos.write("bins", value, NullWritable.get(), "moderate");
		}
		if (cancellationPolicy.equalsIgnoreCase("flexible")) {
			mos.write("bins", value, NullWritable.get(), "flexible");
		}
		if (cancellationPolicy.equalsIgnoreCase("strict")) {
			mos.write("bins", value, NullWritable.get(), "strict");
		}

	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		mos.close();
	}
}
