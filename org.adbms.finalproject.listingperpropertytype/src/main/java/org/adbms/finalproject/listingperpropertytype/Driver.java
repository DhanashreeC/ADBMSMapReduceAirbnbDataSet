package org.adbms.finalproject.listingperpropertytype;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Driver {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		if (args.length != 3) {
			System.err.println("Usage: Number of Room type Group by Property Type <input> <intermediate> <output>");
			System.exit(2);
		}

		Path inputPath = new Path(args[0]);
		Path intermediate = new Path(args[1]);
		Path outputDir = new Path(args[2]);

		Job job = new Job(conf, "NumOfRoomTypeGroupByPropertyType");
		job.setJarByClass(PartitionPatternMapper.class);

		job.setMapperClass(PartitionPatternMapper.class);
		job.setPartitionerClass(GroupbyPropertyTypePartitioner.class);
		GroupbyPropertyTypePartitioner.setPropertyType(job, 1);
		job.setReducerClass(PartitionPatternReducer.class);
		job.setNumReduceTasks(17);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		// Input
		FileInputFormat.addInputPath(job, inputPath);
		job.setInputFormatClass(TextInputFormat.class);

		// Output
		FileOutputFormat.setOutputPath(job, intermediate);

		// Delete output if exists
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(intermediate))
			hdfs.delete(intermediate, true);

		// Execute job
		boolean complete = job.waitForCompletion(true);
		
		Configuration conf2 = new Configuration(true);
		Job job2 = new Job(conf, "NumOfRoomTypeGroupByPropertyType");
		
		if (complete) {
			job2.setJarByClass(RoomTypeMapper.class);

			// Setup MapReduce
			job2.setMapperClass(RoomTypeMapper.class);
			job2.setReducerClass(RoomTypeReducer.class);
			job2.setNumReduceTasks(1);

			// Specify key / value

			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(IntWritable.class);

			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(IntWritable.class);
			// Input
			FileInputFormat.addInputPath(job2, intermediate);
			job2.setInputFormatClass(TextInputFormat.class);

			// Output
			FileOutputFormat.setOutputPath(job2, outputDir);

			// Delete output if exists
			if (hdfs.exists(outputDir))
				hdfs.delete(outputDir, true);

			// Execute job
			int code = job2.waitForCompletion(true) ? 0 : 1;
			System.exit(code);

		}
	}
}
