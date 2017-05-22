import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
public class Country 
{
public static class coun extends Mapper<LongWritable,Text,Text,Text>
{
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException
	{
		String arr[]=value.toString().split(",");
		context.write(new Text(arr[4]), new Text(arr[7]));
	}
}

public static class re extends Reducer<Text,Text,Text,Text>
{
	public void reduce(Text key,Iterable<Text> value,Context context) throws IOException, InterruptedException
	{
 	
		for(Text s:value){
			if(s.toString().contains("New York")){
	 			context.write(key, new Text(s));
	 		}
		}
 		
 		
 	}
	
	}

public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
	Configuration obj=new Configuration();
	Job job=Job.getInstance(obj,"country");
	job.setJarByClass(Country.class);
	job.setMapperClass(coun.class);
job.setReducerClass(re.class);
	//job.setNumReduceTasks(1);
	 job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
     		
	
}
}

