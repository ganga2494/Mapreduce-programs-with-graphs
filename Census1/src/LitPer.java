import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class LitPer {
	  public static class MyMapper extends Mapper<LongWritable,Text,Text,Text>{
	        public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
	            String arr[]=value.toString().split(",");
	            if(!arr[1].toString().contains("Children"))
	            context.write(new Text("male and female %"), new Text(arr[1]+","+arr[3]));
	        }
	    }
	    public static class MyReducer extends Reducer<Text,Text,Text,Text>{
	    	String str;
	        public void reduce(Text key,Iterable<Text> value,Context context) throws IOException, InterruptedException{
	           String gender="";String edu="";
	            double c=0,x=0,p=0;
	            
	            for(Text s:value){
	            	String ss[]=s.toString().split(",");
	                   gender=ss[1];
	                   edu=ss[0];
	                   c++;
	                   if(gender.toString().equals("Male")){
	                	   x++;
	                	   
	                   }
	                   else if(gender.toString().equals("Female")){
	                	   p++;
	                   }
	                   str=(x*100)/c+"  "+(p*100)/c;
	        }
	            context.write(key, new Text(str));
	            
	    }}
	    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
	    Configuration c=new Configuration();
	       
	        Job job=Job.getInstance(c,"Gender wise literacy %%");
	        job.setJarByClass(LitPer.class);
	        job.setMapperClass(MyMapper.class);
	       
	        job.setReducerClass(MyReducer.class);
	        job.setMapOutputKeyClass(Text.class);
	        job.setMapOutputValueClass(Text.class);
	       
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(Text.class);
	        FileSystem.get(c).delete(new Path(args[1]),true);
	        FileInputFormat.addInputPath(job,new Path(args[0]));
	        FileOutputFormat.setOutputPath(job, new Path(args[1]));
	        System.exit(job.waitForCompletion(true)?0:1);



	    }

}
