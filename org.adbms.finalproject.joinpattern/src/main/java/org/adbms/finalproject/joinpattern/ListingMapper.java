package org.adbms.finalproject.joinpattern;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ListingMapper extends Mapper<Object, Text, Text, Text> {

	private Text outkey = new Text();
	private Text outvalue = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] values = value.toString().split(",");
		String listingId = values[0];
		if (listingId.equals("id")) {
			return;
		}
		outkey.set(listingId);
		outvalue.set("L|" + value);
		context.write(outkey, outvalue);
	}
}
