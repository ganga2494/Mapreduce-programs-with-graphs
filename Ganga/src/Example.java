import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public  class Example {
	public static class myclass extends Mapper<LongWritable,Text,Text,IntWritable>{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String name=null;
			int i=1;
			String arr[]=value.toString().split(",");
			for(String a:arr)
			{
				name=a;
				context.write(new Text(name), new IntWritable(i));
			}
			
			}
		public static class MyReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
			public void reduce(Text key,Iterable<IntWritable> value,Context context) throws IOException, InterruptedException{
				int sum=0;
			
				for(IntWritable i:value)
				{
				
				sum=sum+i.get();
				
			}
				context.write(key, new IntWritable(sum));
		}
		}
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration obj=new Configuration();
		Job job=Job.getInstance(obj,"wordcount");
		job.setJarByClass(Example.class);
		job.setMapperClass(myclass.class);
		job.setReducerClass(MyReducer.class);
		
		 job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
         		
		
	
		
	}

}
}