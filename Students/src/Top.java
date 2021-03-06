import java.io.IOException;
import java.util.Iterator;

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




public class Top {
	public static class ma extends Mapper<LongWritable,Text,Text,FloatWritable>
	{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String arr[]=value.toString().split(",");
			context.write(new Text(arr[1]), new FloatWritable(Float.valueOf(arr[5])));
			
		}
	}
	

		
	
	public static class re extends Reducer<Text,FloatWritable,Text,FloatWritable>{
		float mcount=0;
		protected void setup(Context context){
			mcount=0;
		}
	public void reduce(Text key,Iterable<FloatWritable> value,Context context) throws IOException, InterruptedException{
		if(mcount<5){
			try{
				for(FloatWritable values:value){
					context.write(key,values);
					mcount++;
					if(mcount>5)
					{
						break;
					}
				}
			}catch(Exception e){
				
			}
		}
	}
	}
	
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration obj=new Configuration();
		Job job=Job.getInstance(obj,"country");
		job.setJarByClass(Top.class);
		job.setMapperClass(ma.class);
	job.setReducerClass(re.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(FloatWritable.class);

		job.setNumReduceTasks(6);
		 job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(FloatWritable.class);
		    
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    FileSystem.get(obj).delete(new Path(args[1]), true);
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
	     		

	

}


}
