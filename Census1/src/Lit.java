import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class Lit {
	public static class ma extends Mapper<LongWritable,Text,Text,Text>
	{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String arr[]=value.toString().split(",");
			context.write(new Text("total"), new Text(arr[1]));
			
		}
	}
	public static class re extends Reducer<Text,Text,NullWritable,FloatWritable>{
		HashSet hm=new HashSet();
		public void reduce(Text key,Iterable<Text> value,Context context) throws IOException, InterruptedException{
			float count=0.0f;
			float avg=0;
			for(Text a:value){
			String	ab=a.toString();
				if(!ab.equals("Children")){
					count++;
					hm.add(count);
					}
				
				
		}
			
			context.write(NullWritable.get(), new FloatWritable(hm.size()));
			
	}}
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration obj=new Configuration();
		Job job=Job.getInstance(obj,"country");
		job.setJarByClass(Lit.class);
		job.setMapperClass(ma.class);
	job.setReducerClass(re.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(Text.class);
	//	job.setNumReduceTasks(1);
		 job.setOutputKeyClass(NullWritable.class);
		    job.setOutputValueClass(FloatWritable.class);
		    
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    FileSystem.get(obj).delete(new Path(args[1]), true);
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
	     		
		
	}


}
