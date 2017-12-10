package org.adbms.finalproject.topthirtylistingswithmaxavailability;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopThirtyReducer extends Reducer<NullWritable, Text, NullWritable, Text> {

	private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

	@Override
	public void reduce(NullWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		for (Text value : values) {
			String[] row = value.toString().split(",");
			int availability365 = Integer.parseInt(row[4]);
			repToRecordMap.put(availability365, new Text(value));

			if (repToRecordMap.size() > 30) {
				repToRecordMap.remove(repToRecordMap.firstKey());
			}
		}

		for (Text t : repToRecordMap.descendingMap().values()) {
			context.write(NullWritable.get(), t);
		}
	}
}
