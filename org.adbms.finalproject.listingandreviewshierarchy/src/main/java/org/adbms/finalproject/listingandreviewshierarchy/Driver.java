package org.adbms.finalproject.listingandreviewshierarchy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Driver {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: ListingReviewHierarchy <movie> <tag> <outdir>");
			System.exit(1);
		}

		Path outputDir = new Path(args[2]);
		Job job = new Job(conf, "ListingReviewHierarchy");
		job.setJarByClass(Driver.class);

		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, ListingMapper.class);

		MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class, ReviewMapper.class);

		job.setReducerClass(ListingReviewReducer.class);

		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, outputDir);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// Delete output if exists
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputDir))
			hdfs.delete(outputDir, true);

		System.exit(job.waitForCompletion(true) ? 0 : 2);
	}

}
