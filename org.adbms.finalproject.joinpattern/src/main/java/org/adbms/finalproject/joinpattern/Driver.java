package org.adbms.finalproject.joinpattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Driver {
	public static void main(String args[]) throws Exception {

		if (args.length != 3) {
			System.err.println(
					"Usage: Join <input_listings> <input_calendar> <output>");
			System.exit(2);
		}
		Path listingInput = new Path(args[0]);
		Path calenderInput = new Path(args[1]);
		Path outputDir = new Path(args[2]);

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "Join ReduceSide");
		job.setJarByClass(Driver.class);

		MultipleInputs.addInputPath(job, listingInput, TextInputFormat.class, ListingMapper.class);
		MultipleInputs.addInputPath(job, calenderInput, TextInputFormat.class, CalendarMapper.class);
		job.getConfiguration().set("join.type", "inner");
		job.setReducerClass(ListingCalendarReducer.class);

		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, outputDir);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputDir))
			hdfs.delete(outputDir, true);

		System.exit(job.waitForCompletion(true) ? 0 : 2);

	}
}
