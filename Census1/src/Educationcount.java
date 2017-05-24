import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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




public class Educationcount {
	public static class ma extends Mapper<LongWritable,Text,Text,Text>
	{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String arr[]=value.toString().split(",");
			if(!arr[1].contains("Children"))
			context.write(new Text(arr[1]), new Text((arr[1])));
			
		}
	}
	
	public static class re extends Reducer<Text,Text,Text,IntWritable>{
		int count=0;
		
	public void reduce(Text key,Iterable<Text> value,Context context) throws IOException, InterruptedException{
		
		for(Text a:value){
				
			String	ab=a.toString();
			
				count++;
				
		}
		
		context.write(key, new IntWritable(count));
			
		}}
	
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration obj=new Configuration();
		Job job=Job.getInstance(obj,"country");
		job.setJarByClass(Educationcount.class);
		job.setMapperClass(ma.class);
	job.setReducerClass(re.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(Text.class);
	
		//job.setNumReduceTasks(3);
		 job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
		    
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    FileSystem.get(obj).delete(new Path(args[1]), true);
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
	     		

	

}


}
