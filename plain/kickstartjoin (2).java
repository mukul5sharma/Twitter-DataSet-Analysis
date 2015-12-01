import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.*;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.NullWritable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Iterator;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class kickstartjoin 
{
//Job1 
//Mapper 1 
	public static class UserFileMapper extends Mapper <Object, Text, Text, Text>
	{

		//variables to process USerprofile Details
		private String userid,location,statuscount,friendcount,Tag="Userprofile";
		public void map(Object key, Text value, Context context) throws IOException,InterruptedException

		{

			//taking one line/record at a time and parsing them into key value pairs

			String splitarray[] = value.toString().split(",");

			if(splitarray.length == 11)

			{
//reading userid, friendcount, statuscount and location

				userid = splitarray[0].trim();
				friendcount=splitarray[2].trim();
				statuscount=splitarray[4].trim();
				location = splitarray[7].trim();
				context.write(new Text(userid), new Text(Tag+ "," +location+"," +friendcount+"," +statuscount));

			}

		}

	}
//Mapper2
public static class NetworkFileMapper extends Mapper<Object, Text, Text, Text>
{

	//variables to process network data

	private String userid,followerid,Tag="network";
	public void map(Object key, Text value, Context context) throws IOException,InterruptedException
	{

		//taking one line
		String splitarray[] = value.toString().split(",");
		if(splitarray.length == 2)
		{
//reading the userid and the followerid
			followerid = splitarray[0].trim();
			userid = splitarray[1].trim();
			context.write(new Text(userid), new Text(Tag+ "," +followerid));
		}

	}

}
//Reducer 1
public static class ReducerRSJ extends Reducer<Text,Text, Text, Text>
{
	private Text outputtonextmapper = new Text();
	private String location;

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)throws IOException,InterruptedException
	{
		String newlocation="";
		int count=0;
		int friendcount=0;
		int statuscount=0;
		String followerid="";
		ArrayList<String> followerlist = new ArrayList<String>();

		for (Text value : values) 
		{

			String[] arrEntityAttributesList = value.toString().split(",");
if(arrEntityAttributesList.length == 4)

//checking if the record is from user profile or network 
{
			if (arrEntityAttributesList[0].equals("network")) 
			{
				followerid=arrEntityAttributesList[1];
				followerlist.add(followerid);
			}
			else if (arrEntityAttributesList[0].equals("Userprofile")) 
			{
				newlocation = arrEntityAttributesList[1];
				friendcount= Integer.parseInt(arrEntityAttributesList[2]);
				statuscount= Integer.parseInt(arrEntityAttributesList[3]);
				
			}
		}
}
//incrementing the count for each follower of a user
		
		for (String f : followerlist)
		{
			count++;
		}
		outputtonextmapper.set(newlocation+ ","+ count);
//emitting userid as key and location, friendcout,followercount and status count as values
		
		context.write(key, new Text(newlocation+ ","+ count+ ","+friendcount+ ","+statuscount));

	}
}

//Job 2
public static class LocationCountMapper extends Mapper<Object, Text, Text,Text>
{

	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException
	{

		String[] locationlist = value.toString().split(",");
		if (locationlist.length == 4)
		{
			String line = locationlist[0].trim();
			String[] location = line.split("\\s+");
            if(location.length == 2)
            {
			String count = locationlist[1].trim();
			String friendcount=locationlist[2].trim();
			String statuscount=locationlist[3].trim();
//emitting location as key
			context.write(new Text(location[1]),new Text(count+ ","+ friendcount+","+ statuscount));
            }
		}
	}
}


public static class LocationCountReducer extends Reducer<Text, Text, Text, Text> 
{
	private IntWritable result = new IntWritable();
	public void reduce(Text key1, Iterable<Text> values,Context context) throws IOException, InterruptedException 
	{
		//int folowercountperlocation = 0;
		int totalfollower=0;
		int totalfriend=0;
		int totalstatus=0;
		int number =0;
		double afriend,astatus=0.0;
		for (Text val : values)
		{
//calculate totalfollowercount ,average status count and friend count
			String[] arrEntityAttributesList = val.toString().split(",");
if(arrEntityAttributesList.length == 3)
{
			int followercount=Integer.parseInt(arrEntityAttributesList[0].toString());
			int friendcount=Integer.parseInt(arrEntityAttributesList[1].toString());
			int statuscount=Integer.parseInt(arrEntityAttributesList[2].toString());
		totalfollower+=followercount;
		totalfriend+=friendcount;
		totalstatus+=statuscount;
		number++;
}	
		}
		afriend=totalfriend/number;
		astatus=totalstatus/number;

		context.write(key1, new Text(totalfollower+","+ afriend+ ","+ astatus));

	}

}

public static void main(String[] args) throws Exception
{

	Configuration conf = new Configuration();
	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	if (otherArgs.length != 4) 
	{
		System.err.println("Usage: authorAge <in1> <in2> <out>");
		System.exit(2);
	}
//job for join
	Job joinjob = new Job(conf, "location");
	joinjob.setJarByClass(kickstartjoin.class);
	joinjob.setReducerClass(ReducerRSJ.class);
	joinjob.setMapOutputKeyClass(Text.class);
	joinjob.setMapOutputValueClass(Text.class);
	joinjob.setOutputKeyClass(Text.class);
	joinjob.setOutputValueClass(Text.class);
//specifying multiple input files

	MultipleInputs.addInputPath(joinjob, new Path(otherArgs[0]),TextInputFormat.class, UserFileMapper.class);
	MultipleInputs.addInputPath(joinjob, new Path(otherArgs[1]),TextInputFormat.class, NetworkFileMapper.class);
	FileOutputFormat.setOutputPath(joinjob, new Path(otherArgs[2]));
// get result from the job
	boolean result = joinjob.waitForCompletion(true);
//move ahead only if job is completed
	if (joinjob.isSuccessful())
	{
		Job followercount = new Job(conf, "followercount");
		followercount.setJarByClass(kickstartjoin.class);
		followercount.setMapperClass(LocationCountMapper.class);
		followercount.setReducerClass(LocationCountReducer.class);
		followercount.setMapOutputKeyClass(Text.class);
		followercount.setMapOutputValueClass(Text.class);
		followercount.setOutputKeyClass(Text.class);
		followercount.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(followercount, new Path(otherArgs[2]));
		FileOutputFormat.setOutputPath(followercount, new Path(otherArgs[3]));
		System.exit(followercount.waitForCompletion(true) ? 0 : 1);
	}
	else
	{
		System.exit(joinjob.waitForCompletion(true) ? 0 : 1);
	}

}

}