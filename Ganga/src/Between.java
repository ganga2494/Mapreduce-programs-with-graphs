import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class Between {
	public static class myclass extends Mapper<LongWritable,Text,Text,IntWritable>{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
		
			String arr[]=value.toString().split(",");
			
         context.write(new Text(arr[1]), new IntWritable(Integer.parseInt(arr[2])));
				
		
		
		
		}
	}
	public static class MyReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
		

		public void reduce(Text key,Iterable<IntWritable> value,Context context) throws IOException, InterruptedException{
		
	
			for(IntWritable i:value)
			{
			if((i.get()>1945)&(i.get()<1959))
			{
				
				context.write(key, new IntWritable(i.get()));

			
		}
			}
	}
	}
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration obj=new Configuration();
		Job job=Job.getInstance(obj,"wordcount");
		job.setJarByClass(Between.class);
		job.setMapperClass(myclass.class);
		job.setReducerClass(MyReducer.class);
		
		 job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
         		
		
	
		
	}

}

	


	