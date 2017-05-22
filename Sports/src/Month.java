import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class Month {
	public static class ma extends Mapper<LongWritable,Text,Text,FloatWritable>
	{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String arr[]=value.toString().split(",");
       
    
			context.write(new Text(arr[2]), new FloatWritable(Float.valueOf(arr[3])));
			
		}
	}
public static class re extends Reducer<Text,FloatWritable,Text,FloatWritable >{
	float max =0;
	  Text maxWord = new Text();


	public void reduce(Text key,Iterable<FloatWritable> value,Context context) throws IOException, InterruptedException{
		float sum=0.0f;
		for(FloatWritable a:value){
			sum=sum+a.get();
		}
		if(sum > max)
        {
            max = sum;
            maxWord.set(key);
        }
		}
	 protected void cleanup(Context context) throws IOException, InterruptedException {
	      context.write(maxWord, new FloatWritable(max));
	  }
	}

		
	

public static void main(String args[]) throws ClassNotFoundException, IOException, InterruptedException{
	Configuration obj=new Configuration();
	Job job=Job.getInstance(obj,"country");
	job.setJarByClass(Month.class);
	job.setMapperClass(ma.class);
	
     job.setReducerClass(re.class);
job.setMapOutputKeyClass(Text.class);
job.setMapOutputValueClass(FloatWritable.class);
	
	 job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(FloatWritable.class);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    FileSystem.get(obj).delete(new Path(args[1]), true);
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
     		
}

}



