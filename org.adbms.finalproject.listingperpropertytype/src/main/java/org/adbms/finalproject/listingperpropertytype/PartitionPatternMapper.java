package org.adbms.finalproject.listingperpropertytype;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PartitionPatternMapper extends Mapper<Object, Text, IntWritable, Text>{
	private IntWritable outkey = new IntWritable();
	
	public void map(Object key, Text value, Context context
		    ) throws IOException, InterruptedException {
		String[] row = value.toString().split(",");
		
		String propertyType = row[1];
		
		if(!propertyType.equals("property_type")) {
			
			if (propertyType.trim().equals("Apartment")) {
	            outkey.set(1);
	        }
	        else if (propertyType.trim().equals("Bed & Breakfast")) {
	            outkey.set(2);
	        }
	        else if (propertyType.trim().equals("Boat")) {
	            outkey.set(3);
	        }
	        else if (propertyType.trim().equals("Boutique hotel")) {
	            outkey.set(4);
	        }
	        else if (propertyType.trim().equals("Condominium")) {
	            outkey.set(5);
	        }
	        else if (propertyType.trim().equals("Dorm")) {
	            outkey.set(6);
	        }
	        else if (propertyType.trim().equals("Guest suite")) {
	            outkey.set(7);
	        }
	        else if (propertyType.trim().equals("Guesthouse")) {
	            outkey.set(8);
	        }
	        else if (propertyType.trim().equals("Hostel")) {
	            outkey.set(9);
	        }
	        else if (propertyType.trim().equals("House")) {
	            outkey.set(10);
	        }
	        else if (propertyType.trim().equals("In-law")) {
	            outkey.set(11);
	        }
	        else if (propertyType.trim().equals("Loft")) {
	            outkey.set(12);
	        }
	        else if (propertyType.trim().equals("Other")) {
	            outkey.set(13);
	        }
	        else if (propertyType.trim().equals("Serviced apartment")) {
	            outkey.set(14);
	        }
	        else if (propertyType.trim().equals("Timeshare")) {
	            outkey.set(15);
	        }
	        else if (propertyType.trim().equals("Townhouse")) {
	            outkey.set(16);
	        }
	        else if (propertyType.trim().equals("Villa")) {
	            outkey.set(17);
	        }

	        context.write(outkey, value);
			
		}
	}
}
