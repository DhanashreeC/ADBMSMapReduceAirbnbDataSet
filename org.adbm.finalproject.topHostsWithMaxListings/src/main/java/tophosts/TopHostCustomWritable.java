package tophosts;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class TopHostCustomWritable implements Writable, WritableComparable<TopHostCustomWritable>{
	private String hostId;
	private Float listingCount;
	
	public TopHostCustomWritable() {
		
	}

	public TopHostCustomWritable(String m, Float l) {
		this.hostId = m;
		this.listingCount = l;
	}


	public String getHostId() {
		return hostId;
	}

	public void setHostId(String hostId) {
		this.hostId = hostId;
	}


	public Float getListingCount() {
		return listingCount;
	}

	public void setListingCount(Float listingCount) {
		this.listingCount = listingCount;
	}

	public void write(DataOutput d) throws IOException {
		d.writeUTF(hostId);
		d.writeFloat(listingCount);
	}

	public void readFields(DataInput di) throws IOException {
		hostId = di.readUTF();
		listingCount = di.readFloat();
	}
	
	public int compareTo(TopHostCustomWritable o) {
		return -1 * (listingCount.compareTo(o.listingCount));
	}


	@Override
	public String toString() {
		return hostId + "\t" + listingCount;
	}

}