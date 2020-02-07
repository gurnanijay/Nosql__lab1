import java.util.*;
import java.io.IOException;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

public class maxtemp{

	public static class map extends Mapper<LongWritable,Text,Text,IntWritable>{
		private final int MISSING=9999;
		public void map(LongWritable key,Text val,Context c) throws IOException,InterruptedException{
			String s=val.toString();
			int temp;
			String year=s.substring(15,19);
			if(s.charAt(87)=='+')
			{
				temp=Integer.parseInt(s.substring(88,92));
			}
			else
			{
				temp=Integer.parseInt(s.substring(87,92));
			}
			String quality=s.substring(92,93);
			if(temp!=MISSING&&quality.matches("[01495]"))
			{
				c.write(new Text(year),new IntWritable(temp));
			}
		}
	}
	
	public static class reduce extends Reducer<Text,IntWritable,Text,IntWritable>{
		private int max;
		public void reduce(Text key,Iterable<IntWritable> val,Context c) throws IOException,InterruptedException{
			max=Integer.MIN_VALUE;
			for(IntWritable i:val)
			{
				max=Math.max(i.get(),max);
			}
			c.write(key,new IntWritable(max));
		}
	}
	public static void main(String[] args) throws Exception,IOException{
		Configuration conf= new Configuration();
		Job job=Job.getInstance(conf,"maxtemp");
		job.setJarByClass(maxtemp.class);
		job.setMapperClass(map.class);
		job.setReducerClass(reduce.class);
		job.setCombinerClass(reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		Path outpath=new Path(args[1]);
		
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		
		outpath.getFileSystem(conf).delete(outpath,true);
		
		System.exit(job.waitForCompletion(true)?0:1);
		
	}
}



