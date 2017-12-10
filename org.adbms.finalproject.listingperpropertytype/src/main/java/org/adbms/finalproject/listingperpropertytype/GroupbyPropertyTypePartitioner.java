package org.adbms.finalproject.listingperpropertytype;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;

public class GroupbyPropertyTypePartitioner extends Partitioner<IntWritable, Text> implements Configurable {

	private static final String PROPERTY_TYPE = "propertytype";
	private Configuration conf = null;
	private int propertytype = 0;

	public int getPartition(IntWritable key, Text value, int numPartitions) {
		return key.get() - propertytype;
	}

	public Configuration getConf() {
		return conf;
	}

	public void setConf(Configuration conf) {
		this.conf = conf;
		propertytype = conf.getInt(PROPERTY_TYPE, 0);
	}

	public static void setPropertyType(Job job, int type) {
		job.getConfiguration().setInt(PROPERTY_TYPE, type);
	}
}
