
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class AverageDelayKeyGroupingComparator extends WritableComparator 
{
		 
		protected AverageDelayKeyGroupingComparator() {
			super(CompositeKey.class, true);
		}
		 
		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			//Group the records based on the cluster ID
			CompositeKey key1 = (CompositeKey) w1;
			CompositeKey key2 = (CompositeKey) w2;
		 
			return CompositeKey.compare(key1.getcluster(), key2.getcluster());
		}
}
	
