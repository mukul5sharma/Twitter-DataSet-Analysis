

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class AverageDelayKeyPartitioner extends Partitioner<CompositeKey, Text> 
{
		@Override
		public int getPartition(CompositeKey key, Text value, int numPartitions) 
		{
			//partition the data based on cluster ID's hash code
		    try
		    {
				return Math.abs((key.getcluster() + key.getlength()*31*127) % numPartitions);
			} 
		    catch (Exception e) {
				e.printStackTrace();
				return Math.abs((int) (Math.random() * numPartitions)); 
			}
		}
}
