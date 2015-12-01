package kmeansFinal;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class kmeans {
	
	public static String O_input = "";
	public static String O_output = "";
				
	//Create two data structures for storing latitude and longitude values
	public static List<Double> o_centroidsLa = new ArrayList<Double>();
	public static List<Double> o_centroidsLo = new ArrayList<Double>();


	public static class Map extends MapReduceBase implements
							Mapper<LongWritable, Text, Text, Text> {
		/* Setup function */
		@Override
		public void configure(JobConf job) {
			try {
				
				/* Fetch the file from Distributed Cache Read it and store the
				 * centroid in the ArrayList
				 **/
				Path[] cacheFiles = DistributedCache.getLocalCacheFiles(job);
				if (cacheFiles != null && cacheFiles.length > 0) {
					String line;
					o_centroidsLa.clear();
					o_centroidsLo.clear();
					BufferedReader cacheReader = new BufferedReader(
							new FileReader(cacheFiles[0].toString()));
					try {
						
						/* Read the file split by the splitter and store it in
						 * the list
						 **/
						while ((line = cacheReader.readLine()) != null) {
							String[] temp = line.split(",");
							String[] temp1 = temp[0].split("\t");
							
							o_centroidsLa.add(Double.parseDouble(temp1[1]));
							o_centroidsLo.add(Double.parseDouble(temp[1]));
						}
					} 
					finally{
						cacheReader.close();
					}
				}
			} catch (IOException e) {
				System.err.println("Not able to read the DistribtuedCache: " + e);
			}
		}

		public boolean checkValid(Integer[] index,String[] att)
		{
			for(int i=0;i< index.length;i++)
			{
				int j = index[i];
				if(att[j].trim().length() > 0 && att[j] != null)
				{
					return false;
				}
			}
			return true;
		}
		
		/*
		 * Map function will find the minimum center of the point and emit it to
		 * the reducer
		 */
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output,
				Reporter reporter) throws IOException {
			String line = value.toString();

			String[] att = line.split(",");

			double pointla = 0;
			double pointlo = 0;
			String fcount = new String();
			String userName = new String();

			try{
				Integer[] index = new Integer(1,3,9,10);
				if(att.length == 11 && checkValid(index,att))
				{
					pointla = Double.parseDouble(att[10]); 
					pointlo = Double.parseDouble(att[9]);
					fcount = att[3];
					userName = att[1];
					
					// Store the nearest distance between the point and all of the centroids. 
					double smallestDist = Float.MAX_VALUE;

					int c =0; 
					//get minimum and assign nearest center to the point
					for (int i=0; i < 4; i++ )
					{
						double dist = Math.sqrt(Math.pow((pointla - o_centroidsLa.get(i)), 2) 
								+ (Math.pow((pointlo - o_centroidsLo.get(i)), 2))); 

						if (dist < smallestDist )
						{
							c = i;
							smallestDist = dist;
						}
					}

					Text data = new Text();
					data.set(Double.toString(pointla) + "," + Double.toString(pointlo) + "," + fcount + "," + userName);

					String k = Integer.toString(c);

					// Emit the nearest center and the point
					output.collect(new Text(k), data);
				}
			}catch(NumberFormatException nfe)  
			{
				System.out.println("Invalid Integers" + nfe);
			}
		}
	}
	
	/* Reducer */
	
	/*Calculates the average of all the lattitudes and longitudes associated with
	 * one cluster and emits the new values as the new centroids.
	 * */
	public static class Reduce extends MapReduceBase implements
	Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
						throws IOException {
			double x = 0;
			double y = 0;
			double z = 0;
			int count = 0;

			double sumFCount = 0;
			double sumLength = 0;
			int c =0;

			while(values.hasNext()) {
				Text value = values.next();
				String[] latLongGen = value.toString().split(",");
				if(latLongGen.length == 4){
					//Conversion to radians
					double lat = Double.parseDouble(latLongGen[0]) * (Math.PI / 180);
					double lon = Double.parseDouble(latLongGen[1]) * (Math.PI / 180);
					
					//Get followercount and length of username
					double fcount = Double.parseDouble(latLongGen[2]);
					double userNameL = latLongGen[3].length();

					sumFCount = sumFCount + fcount;
					sumLength = sumLength + userNameL; 

					c++;	

					double x_p = Math.cos(lat) * Math.cos(lon);
					double y_p = Math.cos(lat) * Math.sin(lon);
					double z_p = Math.sin(lat);

					x += x_p;
					y += y_p;
					z += z_p;
					count = count + 1;
				}
			}

			x = x/count;
			y = y/count;
			z = z/count;

			//Calculating average
			double avgLon = Math.atan2(y, x) * (180 / Math.PI);
			double hyp = Math.sqrt((x * x) + (y * y));
			double avgLat = Math.atan2(z, hyp) * (180 / Math.PI);

			double avgCount = sumFCount/c;
			double avgL = sumLength/c;

			Text centroidsData = new Text(); 
			centroidsData.set(avgLat + "," + avgLon + "," + avgCount + "," + avgL);

			// Emit the cluster number as key and the new point and average followercount and username length as values
			output.collect(key, centroidsData);
		}
	}

	public static void main(String[] args) throws Exception {
		run(args);
	}
	
	/* Driver Program */
	public static void run(String[] args) throws Exception {
		O_input = args[0];
		O_output = args[1];
		String input = O_input;
		String output = O_output + System.nanoTime();
		String again_input = output;

		// Reiterating till the convergence
		int iteration = 0;
		boolean isdone = false;

		while (isdone == false) {
			JobConf conf = new JobConf(kmeans.class);
			if (iteration == 0) {
				//upload the file to hdfs. 
				Path hdfsPath = new Path(input + "/centroid.txt");
				DistributedCache.addCacheFile(hdfsPath.toUri(), conf);
			} else {
				Path hdfsPath = new Path(again_input + "/part-00000");
				// upload the file to hdfs. Overwrite any existing copy.
				DistributedCache.addCacheFile(hdfsPath.toUri(), conf);
			}

			conf.setJobName("KMeans");
			conf.setMapOutputKeyClass(Text.class);
			conf.setMapOutputValueClass(Text.class);
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);
			conf.setMapperClass(Map.class);
			conf.setReducerClass(Reduce.class);
			conf.setNumReduceTasks(1);
			conf.setInputFormat(TextInputFormat.class);
			conf.setOutputFormat(TextOutputFormat.class);

			//Read the data file
			FileInputFormat.setInputPaths(conf,
					new Path(input + "/samplenewn.txt"));
			FileOutputFormat.setOutputPath(conf, new Path(output));

			JobClient.runJob(conf);

			Path ofile = new Path(output + "/part-00000");
			FileSystem fs = ofile.getFileSystem(new Configuration());

			BufferedReader br = new BufferedReader(new InputStreamReader(
					fs.open(ofile)));
			
			//Assign new centroids
			List<Double> centers_next_la = new ArrayList<Double>();
			List<Double> centers_next_lo = new ArrayList<Double>();
			
			String line = br.readLine();
			
			while (line != null) {
				String[] sp = line.split(",");
				String[] temp1 = sp[0].split("\t");
				double cla = Double.parseDouble(temp1[1]);
				double clo = Double.parseDouble(sp[1]);
				centers_next_la.add(cla);
				centers_next_lo.add(clo);
				line = br.readLine();
			}
			br.close();
			
			//Read Previous Centroids
			String prev;
			if (iteration == 0) {
				prev = input + "/centroid.txt";
			} else {
				prev = again_input + "/part-00000";
			}

			Path prevfile = new Path(prev);
			FileSystem fs1 = prevfile.getFileSystem(new Configuration());

			BufferedReader br1 = new BufferedReader(new InputStreamReader(
					fs1.open(prevfile)));

			List<Double> centers_prev_la = new ArrayList<Double>();
			List<Double> centers_prev_lo = new ArrayList<Double>();
			String l = br1.readLine();
			
			//Read the file again to get the new values
			while (l != null) {
				String[] sp1 = l.split(",");
				String[] temp1 = sp1[0].split("\t");
				double dla = Double.parseDouble(temp1[1]);
				double dlo = Double.parseDouble(sp1[1]);
				centers_prev_la.add(dla);
				centers_prev_lo.add(dlo);
				l = br1.readLine();
			}
			br1.close();

			//Check Convergence condition
			boolean isConverged = true;
			for (int ci=0; ci<4; ci++)
			{
				double distance = Math.sqrt(Math.pow((centers_prev_la.get(ci) - centers_next_la.get(ci)), 2)
						+ (Math.pow((centers_prev_lo.get(ci) - centers_next_lo.get(ci)), 2)));
				
				/*If the euclidean distance between two consecutive centroids calculated is less than 1
				 * convergence condition is said to be met */ 
				if (distance <= 0.1)
				{
					isConverged = isConverged && true;
				}
				else
				{
					isConverged = isConverged && false;
				}

			}

			if (isConverged)
			{
				break;
			}

			++iteration;
			again_input = output;
			output = O_output + System.nanoTime();
		}
	}

}
