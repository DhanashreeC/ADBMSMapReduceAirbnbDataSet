package org.adbms.finalproject.averagepriceperneighborhood;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.PropertyConfigurator;



public class Driver {
	public static void main(String[] args) throws Exception {
		
		
		
		if (args.length != 2) {
			System.err.println("Usage: Average Price per neighborhood <input path> <output path>");
			System.exit(-1);
		}

		Path inputPath = new Path(args[0]);
		Path outputDir = new Path(args[1]);

		// Create configuration
		Configuration conf = new Configuration(true);

		FileSystem hdfs = FileSystem.get(conf);
		
		// Create job
		Job job = new Job(conf, "AveragePricePerNeighborhood");
		job.setJarByClass(NeighborhoodAveragePriceMapper.class);
		

		// Setup MapReduce
		job.setMapperClass(NeighborhoodAveragePriceMapper.class);
		job.setReducerClass(NeighborhoodAveragePriceReducer.class);
		job.setNumReduceTasks(1);

		// Specify key / value
	
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FloatWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		// Input
		FileInputFormat.addInputPath(job, inputPath);
		job.setInputFormatClass(TextInputFormat.class);

		// Output
		FileOutputFormat.setOutputPath(job, outputDir);

		// Delete output if exists
		if (hdfs.exists(outputDir))
			hdfs.delete(outputDir, true);
		
		// Execute job
		int code = job.waitForCompletion(true) ? 0 : 1;
		
		System.exit(code);

	}
}