
package analyzeLogs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedList;

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
import org.apache.hadoop.util.GenericOptionsParser;

public class analyzeLog {
	public static void main(String[] args) throws Exception {

		Configuration c = new Configuration();
		String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
		Path input = new Path(files[0]);
		Path output = new Path(files[1]);
		Job j = new Job(c, "Logger");
		j.setJarByClass(analyzeLog.class);
		j.setNumReduceTasks(1);
		j.setMapperClass(MapForLogger.class);
		j.setReducerClass(ReduceForLogger.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(j, input);
		FileOutputFormat.setOutputPath(j, output);
		System.exit(j.waitForCompletion(true) ? 0 : 1);
	}

	public static class MapForLogger extends Mapper<LongWritable, Text, Text, IntWritable> 
	{

		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException 
		{
			String line = value.toString();
			String[] words = line.split(",");
			if(words[2].contains("logout.php") || words[2].contains("login.php")) {
				//			words[0] contains Ip - Key
				Text outputKey = new Text(words[0].toUpperCase().trim());
				//			words[1] contains Raw Timestamp
				String time[] = words[1].split("[\\[/:]+");
				//				time[0] contains null
				//				time[1] contains date
				//				time[2] contains month
				//				time[3] contains year
				//				time[4] contains hour
				//				time[5] contains minute
				//				time[6] contains second
				int timestamp=Integer.parseInt(time[6]);
				timestamp+=(Integer.parseInt(time[1])*86400);
				timestamp+=(Integer.parseInt(time[3])-2017)*((int)3.154e+7);
				timestamp+=Integer.parseInt(time[4])*3600;
				timestamp+=Integer.parseInt(time[5])*60;
				int month = (int)(2.628e+6);
				int count=0;
				switch (time[2]) {											//Calculate month index
				case "Jan":
					break;
				case "Feb":
					count=+1;
					break;
				case "Mar":
					count=+2;
					break;
				case "Apr":
					count=+3;
					break;
				case "May":
					count=+4;
					break;
				case "Jun":
					count=+5;
					break;
				case "Jul":
					count=+6;
					break;
				case "Aug":
					count=+7;
					break;
				case "Sep":
					count=+8;
					break;
				case "Oct":
					count=+9;
					break;
				case "Nov":
					count=+10;
					break;
				case "Dec":
					count=+11;
					break;
				default:
					count=-1;
					break;
				}
				if(count>-1) {
					timestamp+=count*month;
					//			words[2] contains page details
					if(words[2].contains("GET /logout.php")) {
						IntWritable outputValue = new IntWritable(timestamp);   //If Logout, Pass positive TimeStamp
						con.write(outputKey, outputValue);
					}
					else if(words[2].contains("GET /login.php")) {
						IntWritable outputValue = new IntWritable(-timestamp);	//If Login, Pass negative TimeStamp
						con.write(outputKey, outputValue);
					}
				}
			}
		}
	}

	public static class ReduceForLogger extends Reducer<Text, IntWritable, Text, IntWritable> 
	{
		public static Text key = new Text("");
		public static IntWritable max = new IntWritable(0);
		public void reduce(Text word, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException 
		{
			int sum = 0;										//Variable to Store the Sum of Session Times
			int lastRecord=0;									//Variable to stare the last processed LogOut time during session calculation
			ArrayList<Integer> array = new ArrayList<Integer>();//ArrayList to contain and sort the Iterable values 
			for (IntWritable value : values) 
				array.add(value.get());							//Insert each element of Iterable List into ArrayList 
			Collections.sort(array);							//Sort ArrayList,so First Half(Negative Values) are Login, Second Half(Positive) are LogOut
			Deque<Integer> outTime = new LinkedList<Integer>(); //DQueu to keep track of LogOut TimeStamps
			Deque<Integer> inTime  = new LinkedList<Integer>(); //DQueu to keep track of LogIn TimeStamps
			for (Integer value : array) 
				if(value>0) 									//If positive, i.e logout, push to OutTime
					outTime.add(value);
				else 
					inTime.addFirst(-value);					//If negative, i.e login, convert to positive and push to InTime
			while (!(outTime.isEmpty() || inTime.isEmpty())) {	//If No Queue is Empty then true
				while(inTime.peek()<lastRecord) 				//If inTime's head stores a timestamp earlier than lastRecord
					inTime.pop();								//It is Outdated and pop it out.
				if(outTime.peek() < inTime.peek())				//If outTime's timeStamp is lesser than inTime
					outTime.pop();								//It is Outdated and pop it out.
				else {											//In ALL other Cases
					sum+=outTime.peek() - inTime.pop();			//Update Sum by Difference in Out and InTimes
					lastRecord = outTime.pop();					//Update lastRecord with current outTime head and then pop it
				}
			}
			if(sum>max.get()) {									//Keep track of Max Sum witnessed until now
				key.set(word);
				max.set(sum);
			}
		}
		@Override
		protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			context.write(key,max);								//At End of Reducer Job, Write to OUTPUT
		}
	}
}



