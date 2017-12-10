package org.adbms.finalproject.topthirtylistingswithmaxavailability;

import java.io.IOException;
import java.util.TreeMap;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopThirtyMapper extends Mapper<Object, Text, NullWritable, Text> {

	// Our output key and value Writables

	private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] row = value.toString().split(",");
		
		String listingId = row[0];
		if(listingId.equals("id")) {
			return;
		}

		int availability365 = Integer.parseInt(row[4]);

		repToRecordMap.put(availability365, new Text(value));

		if (repToRecordMap.size() > 30) {
			repToRecordMap.remove(repToRecordMap.firstKey());
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		for (Text t : repToRecordMap.values()) {
			context.write(NullWritable.get(), t);
		}
	}
}
