package org.adbms.finalproject.listingandreviewshierarchy;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ReviewMapper extends Mapper<Object, Text, Text, Text> {

	private Text outKey = new Text();
	private Text outValue = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String[] input = value.toString().split(",");
		if(input.length != 6) {
			return;
		}
		String listingId = input[0];

		String review = input[5];

		outKey.set(listingId);
		outValue.set("R" + review);

		context.write(outKey, outValue);
	}
}
