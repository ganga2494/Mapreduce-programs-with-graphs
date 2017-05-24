import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class Max {
	public static class ma extends Mapper<LongWritable,Text,Text,FloatWritable>
	{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String arr[]=value.toString().split(",");
			if(!arr[1].contains("Children"))
			context.write(new Text(arr[1]), new FloatWritable(Float.valueOf(arr[5])));
			
		}
	}
	public static class Mypartitioner extends Partitioner<Text,FloatWritable>{
		@Override
		public int getPartition(Text key, FloatWritable value, int num) {			
			if(key.toString().contains("High school graduate")){
				return 0;
			}
			if(key.toString().contains("Some college but no degree")){
				return 1;
			}
			if(key.toString().contains("10th grade")){
				return 2;
			}
		
		if(key.toString().contains("Bachelors degree(BA AB BS)")){
			return 3;
		}
				else{
			
			return 4;
		}
		}

		
	}
	public static class re extends Reducer<Text,FloatWritable,Text,FloatWritable>{
	public void reduce(Text key,Iterable<FloatWritable> value,Context context) throws IOException, InterruptedException{
		float min=Integer.MAX_VALUE,max=0;
		for(FloatWritable values:value){
				if(values.get()>max){
					max=values.get();
				}
				if(values.get()<min){
					min=values.get();
				}
		    		
			
				
			}
			context.write(key, new FloatWritable(max));
		
			
		}}
	
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration obj=new Configuration();
		Job job=Job.getInstance(obj,"country");
		job.setJarByClass(Max.class);
		job.setMapperClass(ma.class);
	job.setReducerClass(re.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(FloatWritable.class);
	job.setPartitionerClass(Mypartitioner.class);
		job.setNumReduceTasks(5);
		 job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(FloatWritable.class);
		    
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    FileSystem.get(obj).delete(new Path(args[1]), true);
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
	     		

	

}



}
