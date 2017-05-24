import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class Agewise {
	public static class ma extends Mapper<LongWritable,Text,IntWritable,Text>
	{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String arr[]=value.toString().split(",");
			int i=Integer.parseInt(arr[0]);
			context.write(new IntWritable(i),new Text(arr[1]));
			
		}
	}
	public static class Mypartitioner extends Partitioner<IntWritable,Text>{
		@Override
		public int getPartition(IntWritable key, Text value, int num) {			
			if(key.get()>10 && key.get()<20){
				return 0;
			}
			if(key.get()>20 && key.get()<30){
				return 1;
			}
			if(key.get()>30 && key.get()<40){
				return 2;
			}
		
		if(key.get()>40 && key.get()<120){
			return 3;
		}
		
		else{
			
			return 4;
		}
		}

		
	}
	public static class re extends Reducer<IntWritable,Text,IntWritable,Text>{
		TreeMap hs=new TreeMap();
	public void reduce(IntWritable key,Iterable<Text> value,Context context) throws IOException, InterruptedException{
		float min=Integer.MAX_VALUE,max=0;int count=0;
		for(Text values:value){
				count++;
				if(count>max){
					max=count;
				}
		    	hs.put(max, values);
		    	if(hs.size()>1){
		    		hs.remove(hs.firstKey());
		    	}
			
				
			}
			context.write(key, new Text(hs.toString()));
		
			
		}}
	
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration obj=new Configuration();
		Job job=Job.getInstance(obj,"country");
		job.setJarByClass(Agewise.class);
		job.setMapperClass(ma.class);
	job.setReducerClass(re.class);
	job.setMapOutputKeyClass(IntWritable.class);
	job.setMapOutputValueClass(Text.class);
	job.setPartitionerClass(Mypartitioner.class);
		job.setNumReduceTasks(5);
		 job.setOutputKeyClass(IntWritable.class);
		    job.setOutputValueClass(Text.class);
		    
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    FileSystem.get(obj).delete(new Path(args[1]), true);
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
	     		

	

}



}
