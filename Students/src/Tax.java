import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class Tax {
	public static class ma extends Mapper<LongWritable,Text,Text,FloatWritable>
	{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String arr[]=value.toString().split(",");
			if(!arr[4].toString().contains("Nonfiler"))
			context.write(new Text("total"), new FloatWritable(Float.valueOf(arr[5])));
			
		}
	}
	public static class re extends Reducer<Text,FloatWritable,Text,FloatWritable>{
		
		public void reduce(Text key,Iterable<FloatWritable> value,Context context) throws IOException, InterruptedException{
			float salary=0;
			
			float ab=0;
			
			for(FloatWritable a:value){
				
					salary=a.get()/10;
					
				
			
				
				
			ab=ab+salary;
			
				
			
				
			
			}
			
			context.write(key, new FloatWritable(ab));
		}
	}
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration obj=new Configuration();
		Job job=Job.getInstance(obj,"country");
		job.setJarByClass(Tax.class);
		job.setMapperClass(ma.class);
	job.setReducerClass(re.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(FloatWritable.class);
	//	job.setNumReduceTasks(1);
		 job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(FloatWritable.class);
		    
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    FileSystem.get(obj).delete(new Path(args[1]), true);
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
	     		
		
	}



}
