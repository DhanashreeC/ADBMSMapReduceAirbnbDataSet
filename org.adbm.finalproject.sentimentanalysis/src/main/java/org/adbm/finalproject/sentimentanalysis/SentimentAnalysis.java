package org.adbm.finalproject.sentimentanalysis;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SentimentAnalysis extends Configured implements Tool {

	public static Set<String> goodWords = new HashSet<String>();
	public static Set<String> badWords = new HashSet<String>();

	public static void main(String[] args) throws Exception {

		if (args.length != 4) {
			System.err.println("Usage: Sentiment Analysis <positive> <negative> <input path> <output path>");
			System.exit(-1);
		}

		ToolRunner.run(new SentimentAnalysis(), args);
	}

	public int run(String[] args) throws Exception {

		positiveList(args[0]); // Path to positive words file
		negativeList(args[1]); // Path to negative words file
		Path inputpath = new Path(args[2]);
		Path final_output = new Path(args[3]);
		Configuration conf = new Configuration(true);
		Job job = new Job(conf, "SentimentAnalysis");
		job.setJarByClass(SentimentAnalysis.class);
		job.setMapperClass(SentimentMapper.class);
		job.setReducerClass(SentimentReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, inputpath);
		FileOutputFormat.setOutputPath(job, final_output);

		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(final_output))
			hdfs.delete(final_output, true);

		System.exit(job.waitForCompletion(true) ? 0 : 1);

		return 0;

	}

	private void positiveList(String p) {
		try {
			BufferedReader fis = new BufferedReader(new FileReader(new File(p)));
			String word;
			while ((word = fis.readLine()) != null) {
				goodWords.add(word);
			}
			fis.close();
		} catch (IOException ioe) {
			System.err.println("Caught exception..File not found");
		}

	}

	private void negativeList(String p) {
		try {
			BufferedReader fis = new BufferedReader(new FileReader(new File(p)));
			String word;
			while ((word = fis.readLine()) != null) {
				badWords.add(word);
			}
			fis.close();
		} catch (IOException ioe) {
			System.err.println("Caught exception..File not found");
		}

	}

	public static class SentimentMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String row[] = value.toString().split(",");
			if (row.length < 2) {
				return;
			}
			String listingId = row[0];

			Text listId = new Text(listingId);
			String review = row[1];

			String[] reviewWords = review.toString().split(" ");
			int positive = 0;
			int negative = 0;

			for (String w : reviewWords) {
				if (goodWords.contains(w)) {
					positive++;
				}
				if (badWords.contains(w)) {
					negative++;
				}
			}
			int sentiment_sum = positive - negative;

			context.write(listId, new IntWritable(sentiment_sum));

		}
	}

	public static class SentimentReducer extends Reducer<Text, IntWritable, Text, Text> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			int totalsentiment = 0;
			Text result = new Text("NEUTRAL");
			for (IntWritable val : values) {
				totalsentiment += val.get();

			}

			if (totalsentiment > 0) {
				result.set("POSITIVE");
			} else if (totalsentiment < 0) {
				result.set("NEGATIVE");
			}
			context.write(key, result);
		}
	}

}
