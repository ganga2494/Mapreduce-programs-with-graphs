import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
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




public class Letter {
	

	

	

	public static class myclass extends Mapper<LongWritable,Text,IntWritable,Text>{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
	
	
			String arr[]=value.toString().split(",");
			
				
				
					context.write(new IntWritable(Integer.parseInt(arr[2])),new Text(arr[1]));
				}
				}
		public static class re extends Reducer<IntWritable,Text,IntWritable,Text>{
			public void reduce(IntWritable key,Iterable<Text> value,Context context) throws IOException, InterruptedException{
				for(Text a:value){
					if(a.toString().contains("t")){
						context.write(key, new Text(a));
					}
				}
			}
		}
		
			
			
			public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
				Configuration obj=new Configuration();
				Job job=Job.getInstance(obj,"letter");
				job.setJarByClass(Letter.class);
				job.setMapperClass(myclass.class);
				job.setReducerClass(re.class);
				job.setOutputKeyClass(IntWritable.class);
				    job.setOutputValueClass(Text.class);
				    FileInputFormat.addInputPath(job, new Path(args[0]));
				    FileOutputFormat.setOutputPath(job, new Path(args[1]));
				    System.exit(job.waitForCompletion(true) ? 0 : 1);
		         		
}
	}

