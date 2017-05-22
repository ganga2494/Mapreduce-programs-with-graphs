import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class Rate {
	public static class ganga extends Mapper<LongWritable,Text,Text,FloatWritable>{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			
			String arr[]=value.toString().split(",");
			
			
         context.write(new Text(arr[1]), new FloatWritable(Float.valueOf(arr[3])));
				
		

}
	}
	public static class ravi extends Reducer<Text,FloatWritable,Text,FloatWritable>{
		

		public void reduce(Text key,Iterable<FloatWritable> value,Context context) throws IOException, InterruptedException{
		
	
			for(FloatWritable i:value)
			{
			if(i.get()>3.9)
			{
				
				context.write(key,new FloatWritable(i.get()));

			
		}
			}
	}
	}
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration obj=new Configuration();
		Job job=Job.getInstance(obj,"rate");
		job.setJarByClass(Rate.class);
		job.setMapperClass(ganga.class);
		
		job.setReducerClass(ravi.class);
		
		 job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(FloatWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	
		
	}

}

	





	
