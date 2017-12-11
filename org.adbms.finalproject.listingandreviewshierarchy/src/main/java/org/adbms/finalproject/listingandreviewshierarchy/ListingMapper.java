package org.adbms.finalproject.listingandreviewshierarchy;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class ListingMapper extends Mapper<Object,Text,Text,Text> {

    private Text outKey=new Text();
    private Text outValue=new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String[] input=value.toString().split(",");
        String listingId=input[0];
        if(listingId.equals("id"))
            return;
        outKey.set(listingId);
        outValue.set("L"+listingId);

        context.write(outKey,outValue);
    }
}
