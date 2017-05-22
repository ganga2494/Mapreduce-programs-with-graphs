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




public class Credit {
	public static class cre extends Mapper<LongWritable,Text,Text,Text>{
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String arr[]=value.toString().split(",");
		String	date=arr[1];
		int txn=Integer.parseInt(arr[2]);
		
		String gamet=arr[4];
		String game=arr[5];
		String city=arr[6];
		String country=arr[7];
		String str=date+""+txn+""+gamet+""+game+""+city+""+country;
		
			context.write(new Text(str), new Text(arr[8]));

		}
	}
public static class red extends Reducer<Text,Text,Text,Text>{
	public void reduce(Text key,Iterable<Text> value,Context context) throws IOException, InterruptedException{
		

		for(Text i:value){
			
			if(i.toString().contains("cash")){
				context.write(key, new Text(i));
				
				
			}
		}
	}}

public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException{
	Configuration obj=new Configuration();
	Job job=Job.getInstance(obj,"credit");
	job.setJarByClass(Credit.class);
	job.setMapperClass(cre.class);
	job.setReducerClass(red.class);
	
	 job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
     		
	
}
}
