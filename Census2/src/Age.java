import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class Age {
	public static class Mymapper extends Mapper< LongWritable,Text,IntWritable,IntWritable>
	{
		public void map(LongWritable Key , Text value , Context context) throws IOException, InterruptedException
		{
		  String arr[]=value.toString().split(",");
		
	
		  int i = Integer.parseInt(arr[0]);
		  if(i>21 &&i<25)
		 
		
		 
		context.write( new IntWritable(i),new IntWritable(i));
			  }
	}
	public static class MyReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>
	{
		public void reduce(IntWritable key, Iterable<IntWritable> value,Context context) throws IOException, InterruptedException
		{    
			int count=0;
			for(IntWritable K:value)
			{
				
					count++;
					
				
			
			}
			context.write(key,new IntWritable (count));
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration c = new Configuration ();
	    
		Job job=Job.getInstance(c,"red");
		
		job.setJarByClass(Age.class);
		job.setReducerClass(MyReducer.class);
		job.setMapperClass(Mymapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileSystem.get (c).delete(new Path(args[1]),true); /// no need to change the op1 always 
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}


}
