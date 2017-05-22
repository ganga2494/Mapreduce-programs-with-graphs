import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class SC {
	public static class ba extends Mapper<LongWritable,Text,IntWritable,FloatWritable>{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String arr[]=value.toString().split(",");
			
			
			context.write(new IntWritable(Integer.parseInt(arr[2])), new FloatWritable(Float.valueOf(arr[3])));
		}
	}
	public static class at extends Reducer<IntWritable,FloatWritable,IntWritable,Text>{
		

		public void reduce(IntWritable key,Iterable<FloatWritable> value,Context context) throws IOException, InterruptedException{
			float count=0.0f;
			float sum=0.0f;
			
			for (FloatWritable a:value){
				
			
				sum=sum+a.get();
				count++;
				
			}

			String d=sum+" "+count;
			
			context.write(key, new Text(d));

		}
	}
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration obj=new Configuration();
		Job job=Job.getInstance(obj,"country");
		job.setJarByClass(SC.class);
		job.setMapperClass(ba.class);
	job.setReducerClass(at.class);
	job.setMapOutputKeyClass(IntWritable.class);
	job.setMapOutputValueClass(FloatWritable.class);
		//job.setNumReduceTasks(1);
		 job.setOutputKeyClass(IntWritable.class);
		    job.setOutputValueClass(Text.class);
		    
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    FileSystem.get(obj).delete(new Path(args[1]), true);
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
	     		
		
	}
	}


