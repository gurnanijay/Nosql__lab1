import java.util.*;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;

public class URLNotFound{


	public static class map extends Mapper<LongWritable,Text,Text,Text>
	{
	
	public void map(LongWritable key,Text val,Context c) throws IOException,InterruptedException
	{
		String url= val.toString().split(" ")[6];
		String status= val.toString().split(" ")[8];
		if(status.equals("404"))
		{
			String datetime=val.toString().split(" ")[3].substring(1);
			String year=datetime.substring(7,11);
			String date=datetime.substring(0,2);
			String month=datetime.substring(3,6);
			String hh=datetime.substring(12,14);
			String mm=datetime.substring(15,17);
			String ss=datetime.substring(18,20);
			String zone=val.toString().split(" ")[4].substring(0,5);
			if(month.equals("Jan"))month="01";else if(month.equals("Feb"))month="02";else if(month.equals("Mar"))month="03";else if(month.equals("Apr"))month="04";else if(month.equals("May"))month="05";else if(month.equals("Jun"))month="06";else if(month.equals("Jul"))month="07";else if(month.equals("Aug"))month="08";else if(month.equals("Sep"))month="09";else if(month.equals("Oct"))month="10";else if(month.equals("Nov"))month="11";else if(month.equals("Dec"))month="12";
		
			String ts=year+"-"+month+"-"+date+"T"+hh+":"+mm+":"+ss+zone.substring(0,3)+":"+zone.substring(3);
			c.write(new Text(ts),new Text(url));
		}
		
		
	}
	}
	
	public static void main(String args[]) throws Exception,IOException
	{
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf,"URL Not Found");
		job.setJarByClass(URLNotFound.class);
		job.setMapperClass(map.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		Path outpath =new Path(args[1]);
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,outpath);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(0);
		
		outpath.getFileSystem(conf).delete(outpath,true);
		System.exit(job.waitForCompletion(true)?0:1);
	}
	
}

