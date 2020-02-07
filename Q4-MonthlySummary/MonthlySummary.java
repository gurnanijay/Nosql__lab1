import java.util.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;

public class MonthlySummary
{
	public static class IntPair implements Writable
	{
		private IntWritable first;
		private IntWritable second;
		
		public IntPair()
		{
			first=new IntWritable();
			second=new IntWritable();
		}
		public IntPair(int f,int s)
		{
			first=new IntWritable();
			second=new IntWritable();
			this.setFirst(f);this.setSecond(s);
		}
		public IntWritable getFirst()
		{
			return first;
		}
		public IntWritable getSecond()
		{
			return second;
		}
		public void setFirst(int f)
		{
			this.first.set(f);
		}
		public void setSecond(int s)
		{
			this.second.set(s);
		}
		@Override
		public void readFields(DataInput in) throws IOException
		{
			first.readFields(in);
			second.readFields(in);
		}
		
		@Override
		public void write(DataOutput in) throws IOException
		{
			first.write(in);
			second.write(in);
		}
		@Override
		public String toString()
		{
			return first.toString() +" "+ second.toString();
		}
		@Override
		public boolean equals(Object o)
		{
			 if(o instanceof IntPair)
        		{
			    IntPair tp = (IntPair) o;
			    return first.equals(tp.first) && second.equals(tp.second);
			}
			return false;
		}
		IntPair plus (Object o)
		{
			if(o instanceof IntPair)
			{
				IntPair tp=(IntPair) o;
				IntPair i=new IntPair(first.get()+tp.first.get(),second.get()+tp.second.get());
				return i;
			}
			else
			return null;
		}
		
		
	}
	public static class map extends Mapper<LongWritable,Text,Text,IntPair>
	{
		private static int j=0;
		public void map(LongWritable key,Text val,Context c) throws IOException, InterruptedException
		{
			
			String ts =val.toString().split(" ")[3];
			//System.out.println(ts);
			String month= ts.substring(4,12);
			int k=month.lastIndexOf("/");
			month=month.substring(0,k)+"-"+month.substring(k+1);
			IntPair i=new IntPair();
			String objectsize =val.toString().split(" ")[9];
			i.setFirst(1);
			if(objectsize.equals("-"))
			{
				i.setSecond(0);
			}
			else
			{int data=Integer.parseInt(objectsize);
			i.setSecond(data);
			}
			//System.out.println(i);
			c.write(new Text(month), i);
		}
	}
	public static class reduce extends Reducer<Text,IntPair,Text,IntPair>
	{
		public void reduce(Text key,Iterable<IntPair> val,Context c) throws IOException, InterruptedException
		{
			
			IntPair sum=new IntPair(0,0);
			for(IntPair i:val)
			{
				//System.out.println(i);
				sum.setFirst(sum.getFirst().get()+i.getFirst().get());
				sum.setSecond(sum.getSecond().get()+i.getSecond().get());
			}
			//System.out.println(sum);
			c.write(key,sum);
		}
	}
	public static void main(String[] args) throws Exception,IOException
	{
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf,"Monthly Summary");
		job.setJarByClass(MonthlySummary.class);
		job.setMapperClass(map.class);
		job.setCombinerClass(reduce.class);
		job.setReducerClass(reduce.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntPair.class);
		
		Path outputPath= new Path(args[1]);
		
		FileInputFormat.addInputPath(job,new Path(args[0]));
		//FileInputFormat.addInputPath(job,new Path(args[1]));
		FileOutputFormat.setOutputPath(job,outputPath);
		
		outputPath.getFileSystem(conf).delete(outputPath,true);
		
		System.exit(job.waitForCompletion(true)?0:1);
		
		
	}

}
