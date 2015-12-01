
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


//Composite key consists of the cluster ID and the length
public class CompositeKey implements WritableComparable<CompositeKey>{
	public int cluster;
	public int length;
	
	public CompositeKey(){
		this.cluster = 0;
		this.length = 0;
	}
	
	public 
	CompositeKey(int cluster, int length){
		this.cluster = cluster;
		this.length = length;
	}
	
	@Override
	public String toString() {
	 return (new StringBuilder()).append(cluster).append(',').append(length).toString();
	}
	
	public void setcluster(int cluster){
		this.cluster = cluster;
	}
	
	public void setlength(int length){
		this.length = length;
	}
	
	public int getcluster(){
		return cluster;
	}
	
	public int getlength(){
		return length;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		cluster = in.readInt();
		length = in.readInt();
	}
	 
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(cluster);
		out.writeInt(length);
	}
	
	@Override
	public boolean equals(Object o) {
		if (o instanceof CompositeKey) {
			CompositeKey tp = (CompositeKey) o;
			return cluster == tp.cluster && length == tp.length;
	}
		return false;
	}
	
	@Override
	public int compareTo(CompositeKey o) {
		int result = cluster == o.cluster? 0 : 1;
//		int result = cluster.compareTo(o.cluster);
		if (0 == result) {
			result = length == o.length? 0 : 1;
		}
		return result;
	}
	
	
	
	@Override
	public int hashCode(){
		int result = cluster;
		result = 31 * result + length;
		return result;
	}

	public static int compare(int length, int length2) {
		if (length == length2) {
			return 0;
		}
		return length > length2 ? 1 : -1;
	}

}
