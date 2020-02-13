package weather;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTemperature
{
	public static void main(String[] args) throws Exception
	{
		if(args.length != 2)
		{
			System.err.println("Usage:MaxTemperature<input path><output path>");
			System.exit(-1);
		}
		Job job= new Job();
		job.setJarByClass(MaxTemperature.class);
		job.setJobName("Max temperature");
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		job.setMapperClass(MaxTemperatureMapper.class);
		job.setReducerClass(MaxTemperatureReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.waitForCompletion(true);
	}
	
	public static class MaxTemperatureMapper extends Mapper<LongWritable,Text,Text,IntWritable>
	{
		private static final int MISSING= 9999;
		public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException
		{
			String line = value.toString();
			String year = line.substring(15, 19);
			int airTemperature;
			if (line.charAt(87)=='+')
				airTemperature= Integer.parseInt(line.substring(88, 92));
			else
				airTemperature= Integer.parseInt(line.substring(87, 92));
			
			if (airTemperature != MISSING)
				context.write(new Text(year), new IntWritable(airTemperature));
		}
	}
	
	public static class MaxTemperatureReducer extends Reducer<Text,IntWritable,Text,IntWritable>
	{
		public static IntWritable maxValue = new IntWritable(Integer.MIN_VALUE);
		public static Text maxYear = new Text("");
		public static IntWritable minValue = new IntWritable(Integer.MAX_VALUE);
		public static Text minYear = new Text("");
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,InterruptedException
		{
			int sum = 0,i=0;
			for(IntWritable val : values)
			{
				sum+=val.get();
				i++;
			}
			sum/=i;
			if(sum > maxValue.get())
			{
				maxValue.set(sum);
				maxYear.set(key);
			}
			
			if(sum < minValue.get())
			{
				minValue.set(sum);
				minYear.set(key);
			}		
		}
		
		@Override protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException 
		{
			context.write(maxYear,maxValue);
			context.write(minYear,minValue);
		}
	}
}










