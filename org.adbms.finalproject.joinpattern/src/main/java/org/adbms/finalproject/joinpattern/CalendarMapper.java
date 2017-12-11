package org.adbms.finalproject.joinpattern;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CalendarMapper extends Mapper<Object, Text, Text, Text> {

	private Text outkey = new Text();
	private Text outvalue = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] rows = value.toString().split(",");
		String listingId = rows[0];
		if (listingId.equals("listing_id")) {
			return;
		}
		outkey.set(listingId);
		outvalue.set("C|" + value);
		context.write(outkey, outvalue);
	}
}
