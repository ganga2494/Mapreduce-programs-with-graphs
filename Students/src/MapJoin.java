import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MapJoin {
	public static class MyMapper extends Mapper<LongWritable,Text,LongWritable,Text>
	{
		HashMap<Long,String> userdata=new HashMap<Long,String>();
		LongWritable outkey=new LongWritable();
		Text outvalue=new Text();
		public void setup(Context con) 
		{
			try{
			Path files[]=DistributedCache.getLocalCacheFiles(con.getConfiguration());
			for(Path f:files)
			{
				if(f.getName().equals("students.dat"))
				{
					BufferedReader br=new BufferedReader(new FileReader(f.toString()));
					
					String line=br.readLine();
					
						String str[]=line.split(",");
						long id=Long.parseLong(str[1]);
						String name=str[0];
						
						userdata.put(id, name);
						line=br.readLine();
					
					br.close();
					
				}
				
			}
		}
		catch(Exception e)
		
		{
			System.err.println(e);
		}
		}
		public void map(LongWritable key,Text value,Context con) throws IOException, InterruptedException
		{
			String str[]=value.toString().split(",");
			long id=Long.parseLong(str[0]);
			String mr=userdata.get(id);
			String status=str[1];
			
			
			String out=status+mr;
			outvalue.set(out);
			outkey.set(id);
			
			con.write(outkey,outvalue);
		}
	}
	public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException 
	{
		Configuration cfg=new Configuration();
		Job job=Job.getInstance(cfg,"Mapsidejoin");
	DistributedCache.addCacheFile(new URI("/user/hadoop/students.dat"), cfg);  
		job.setJarByClass(MapJoin.class);
		job.setMapperClass(MyMapper.class);
		
        job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job,new Path(args[0]));
		 FileSystem.get(cfg).delete(new Path(args[1]),true);
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		  System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}


