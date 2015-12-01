

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class AverageDelayKeyComparator extends WritableComparator {
		
		public AverageDelayKeyComparator() {
			super(CompositeKey.class, true);
		}
		
		//Copare using the natural key and then the length but natural key is the same
		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			
			CompositeKey key1 = (CompositeKey) w1;
			CompositeKey key2 = (CompositeKey) w2;
		 
		
			int compare = CompositeKey.compare(key1.getcluster(), key2.getcluster());
		 
			if (compare == 0) {
				return CompositeKey.compare(key1.getlength(),key2.getlength());
			}
		 
			return compare;
		}
	}
