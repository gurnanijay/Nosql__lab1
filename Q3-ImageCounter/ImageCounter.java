import java.util.*;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;

public class ImageCounter{

	public static class map extends Mapper<LongWritable,Text,Text,IntWritable>
	{
	
		private final HashSet<String> isImg= new HashSet<String>(new ArrayList<String>(Arrays.asList("jpeg","jpg","png","jif","jfif","jfi","gif","webp","tiff","tif","psd","raw","arw","cr2","nrw","k25","bmp","dib","heif","heic","ind","indd","indt","jp2","j2k","jpx","jpm","mj2")));
		private final HashSet<String> isJPG= new HashSet<String>(new ArrayList<String>(Arrays.asList("jpeg","jpg","jif","jfif","jfi")));
		
		public void map(LongWritable key,Text val,Context c) throws IOException,InterruptedException
		{
			
			String request=val.toString();
			String url= request.split(" ")[6];
			String method= request.split(" ")[5];
			System.out.println(method);
			int i=url.lastIndexOf(".");
			
			if(i!=-1)
			{
				
			
			String extension=url.substring(i+1);
			
			//System.out.println(extension);
			
			
			if(isImg.contains(extension)&&method.equals("\"GET"))
			{
				if(extension.equals("gif"))
				{
					c.write(new Text("GIF Images"),new IntWritable(1));
					c.write(new Text("JPG Images"),new IntWritable(0));
					c.write(new Text("Other Images"),new IntWritable(0));
				}
				else if(isJPG.contains(extension))
				{
					c.write(new Text("JPG Images"),new IntWritable(1));
					c.write(new Text("GIF Images"),new IntWritable(0));
					c.write(new Text("Other Images"),new IntWritable(0));
				}
				else
				{
					c.write(new Text("Other Images"),new IntWritable(1));
					c.write(new Text("GIF Images"),new IntWritable(0));
					c.write(new Text("JPG Images"),new IntWritable(0));
				}
			}}
			else
			{
					c.write(new Text("Other Images"),new IntWritable(0));
					c.write(new Text("GIF Images"),new IntWritable(0));
					c.write(new Text("JPG Images"),new IntWritable(0));
			}
			
			
		}
	}
	public static class reduce extends Reducer<Text,IntWritable,Text,IntWritable>
	{
		public void reduce(Text key, Iterable<IntWritable> val,Context c) throws IOException,InterruptedException
		{
			int sum=0;
			for(IntWritable i:val)
			{
				sum+=i.get();
			}
			c.write(key,new IntWritable(sum));
		}
	}
	public static void main(String[] args) throws Exception,IOException
	{
		Configuration conf =new Configuration();
		Job job=Job.getInstance(conf,"Image Counter");
		job.setJarByClass(ImageCounter.class);
		job.setMapperClass(map.class);
		job.setReducerClass(reduce.class);
		job.setCombinerClass(reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		Path outpath=new Path(args[1]);
		
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,outpath);
		outpath.getFileSystem(conf).delete(outpath,true);
		System.exit(job.waitForCompletion(true)?0:1);
		
	}
}
