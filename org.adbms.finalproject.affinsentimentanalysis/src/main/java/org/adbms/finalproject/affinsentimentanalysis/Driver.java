package org.adbms.finalproject.affinsentimentanalysis;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver extends Configured implements Tool {

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private URI[] files;
		private HashMap<String, String> AFINN_map = new HashMap<String, String>();

		public void setup(Context context) throws IOException {

			files = DistributedCache.getCacheFiles(context.getConfiguration());
			Path path = new Path(files[0]);
			FileSystem fs = FileSystem.get(context.getConfiguration());
			FSDataInputStream in = fs.open(path);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String line = "";
			while ((line = br.readLine()) != null) {
				String splits[] = line.split("\t");
				AFINN_map.put(splits[0], splits[1]);
			}
			br.close();
			in.close();
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String listingId;
			String review;
			String rows[] = value.toString().split(",");
			if (rows.length < 2) {
				return;
			}
			listingId = rows[0];
			review = rows[1];
			String[] splits = review.split(" ");
			int sentimentSum = 0;
			for (String word : splits) {
				if (AFINN_map.containsKey(word)){
					Integer count = new Integer(AFINN_map.get(word));
					sentimentSum += count;
				}
			}
			context.write(new Text(listingId), new IntWritable(sentimentSum));

		}

		public boolean isAlpha(String name) {
			return name.matches("[a-zA-Z]+");
		}

	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, Text> {

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

			int totalsentiment = 0;
			Text result = new Text("NEUTRAL");
			for (IntWritable val : values) {
				totalsentiment += val.get();

			}
			if (totalsentiment >= 401) {
				result.set(totalsentiment + "\tPOSITIVE");
			} else if (totalsentiment < 401 && totalsentiment >= 100 ) {
				result.set(totalsentiment + "\tNEUTRAL");
			}else if(totalsentiment < 100) {
				result.set(totalsentiment + "\tNEGATIVE");
			}
			context.write(key, result);

		}

	}

	public static void main(String[] args) throws Exception

	{

		ToolRunner.run(new Driver(), args);

	}

	public int run(String[] args) throws Exception {

		// TODO Auto-generated method stub

		Configuration conf = new Configuration();

		if (args.length != 3) {

			System.err.println("Usage: Parse <affin> <in> <out>");

			System.exit(2);

		}

		DistributedCache.addCacheFile(new URI(args[0]), conf);
		Job job = new Job(conf, "SentimentAnalysis");
		job.setJarByClass(Driver.class);

		job.setMapperClass(Map.class);

		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(Text.class);

		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);

		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[1]));

		Path outputDir = new Path(args[2]);
		FileOutputFormat.setOutputPath(job, outputDir);
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputDir))
			hdfs.delete(outputDir, true);

		System.exit(job.waitForCompletion(true) ? 0 : 1);

		return 0;

	}
}
