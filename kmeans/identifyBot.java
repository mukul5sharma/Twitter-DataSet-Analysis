

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class identifyBot {

	static double avgZeroClusterL[] = {10.69943724534711,10.700405837308914,10.69560048921554,10.70156964479034};

	static double avgZeroCFollowerC[] = {1361.1467997221025,1312.9744519321791,1378.830677065672,1356.8234466385238};

	public static class createDataMapper extends Mapper<LongWritable, Text, CompositeKey, Text> {

		public CompositeKey key1 = new CompositeKey();
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString();
			String[] att = line.split(",");
			
			double avgfCount = 0;
			double avgL = 0;
			int cluster = 0; 
			double distFC = 0;
			double distL = 0;
			String c ="0";
			String name = "";
			try{
				if(att.length == 6){
					avgfCount = Double.parseDouble(att[3]); 
					avgL = Double.parseDouble(att[4]);
					cluster = Integer.parseInt(att[0]);
					name = att[5];
				}
			}catch(NumberFormatException nfe)  
			{}			


			if(cluster == 0)
			{
				distFC = avgZeroCFollowerC[0] - avgfCount;
				distL = avgZeroClusterL[0] - avgL;
				c = "0";
			}else if(cluster == 1)
			{
				distFC = avgZeroCFollowerC[1] - avgfCount;
				distL = avgZeroClusterL[1] - avgL;
				c = "1";
			}
			else if(cluster == 2)
			{
				distFC = avgZeroCFollowerC[2] - avgfCount;
				distL = avgZeroClusterL[2] - avgL;
				c = "2";
			}else 
			{
				distFC = avgZeroCFollowerC[3] - avgfCount;
				distL = avgZeroClusterL[3] - avgL;
				c = "3";
			} 


			String para = c + "," + distFC + "," + distL + "," + name;
			
			Text v = new Text(para); 
			key1.setcluster(Integer.parseInt(c));
			key1.setlength((int)distL);
			context.write(key1,v);
		}

	}

	
	public static class botReducer extends Reducer<CompositeKey,Text,
	CompositeKey,Text> {
		
		int i =0;
		
		public void reduce(CompositeKey key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			for(Text vItem : values){
				if(i<6)
				{
					System.out.println(vItem + "\n");
					i++;
				}
				context.write(key, vItem);
				
			}
			
		}
	}
	

public static void main( String[] args ) throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Bot Detect");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: Plain <input> <output>");
			System.exit(2);
		}
		job.setJarByClass(identifyBot.class);
		job.setMapperClass(createDataMapper.class);
		job.setReducerClass(botReducer.class);
		job.setOutputKeyClass(CompositeKey.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setNumReduceTasks(4);
		job.setPartitionerClass(AverageDelayKeyPartitioner.class);
		job.setGroupingComparatorClass(AverageDelayKeyGroupingComparator.class);
		job.setSortComparatorClass(AverageDelayKeyComparator.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		boolean result = job.waitForCompletion(true);

		System.exit(result ? 0 : 1);
	}

}
