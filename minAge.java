import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class minAge {
	
	// Job1 To find the minimum age of direct friends

	public static class MapClass_job1 extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
			String[] friends = values.toString().split("\t");
			if (friends.length == 2) {
				context.write(new Text(friends[0]), new Text(friends[1]));
			}
		}
	}

	public static class Reducer_job1 extends Reducer<Text, Text, Text, Text> {

		private int calculateAge(String s) throws ParseException {

			Calendar today = Calendar.getInstance();
			SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");
			Date date = sdf.parse(s);
			Calendar dob = Calendar.getInstance();
			dob.setTime(date);

			int curYear = today.get(Calendar.YEAR);
			int dobYear = dob.get(Calendar.YEAR);
			int age = curYear - dobYear;

			int curMonth = today.get(Calendar.MONTH);
			int dobMonth = dob.get(Calendar.MONTH);
			if (dobMonth > curMonth) { // this year can't be counted!
				age--;
			} else if (dobMonth == curMonth) { // same month? check for day
				int curDay = today.get(Calendar.DAY_OF_MONTH);
				int dobDay = dob.get(Calendar.DAY_OF_MONTH);
				if (dobDay > curDay) { // this year can't be counted!
					age--;
				}
			}
			return age;
		}

		static HashMap<Integer, Integer> map = new HashMap<Integer, Integer>();

		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			Configuration conf = context.getConfiguration();
			Path part = new Path(conf.get("Data"));// Location of file in HDFS
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] fss = fs.listStatus(part);
			for (FileStatus status : fss) {
				Path pt = status.getPath();
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
				String line;
				line = br.readLine();
				while (line != null) {
					String[] info = line.split(",");
					if (info.length == 10) {
						try {
							int age = calculateAge(info[9]);
							map.put(Integer.parseInt(info[0]), age);
						} catch (ParseException e) {
							e.printStackTrace();
						}
					}
					line = br.readLine();
				}
			}
		}

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text tuples : values) {
				String[] friendsList = tuples.toString().split(",");
				int minimumAge = 1000;
				int age;
				for (String eachFriend : friendsList) {
					age = map.get(Integer.parseInt(eachFriend));
					if (age < minimumAge) {
						minimumAge = age;
					}
				}
				context.write(key, new Text(Integer.toString(minimumAge)));
			}
		}
	}

	//Job2 arrange the data by decreasing values of minimim age
	public static class MapClass_job2 extends Mapper<LongWritable, Text, LongWritable, Text> {
		private LongWritable count = new LongWritable();

		public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
			String[] info = values.toString().split("\t");
			if (info.length == 2) {
				count.set(Long.parseLong(info[1]));
				context.write(count, new Text(info[0]));
			}
		}
	}

	public static class Reducer_job2 extends Reducer<LongWritable, Text, Text, Text> {
		public void reduce(LongWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text tuples : values) {
				context.write(tuples, new Text(Long.toString(key.get())));
			}
		}
	}
	
	//Job3 Only top 10 values 

	public static class MapClass_job3 extends Mapper<LongWritable, Text, Text, Text> {
		private int count = 0;

		public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
			if (count < 10) {
				String[] info = values.toString().split("\t");
				if (info.length == 2) {
					count++;
					context.write(new Text(info[0]), new Text(info[1]));
				}
			}
		}
	}
	
	public static class Reducer_job3 extends Reducer<Text, Text, Text, Text> {
		static HashMap<Integer, String> map = new HashMap<Integer, String>();

		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			Configuration conf = context.getConfiguration();
			Path part = new Path(conf.get("Data"));// Location of file in HDFS
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] fss = fs.listStatus(part);
			for (FileStatus status : fss) {
				Path pt = status.getPath();
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
				String line;
				line = br.readLine();
				while (line != null) {
					String[] info = line.split(",");
					if (info.length == 10) {
						map.put(Integer.parseInt(info[0]), info[1]+","+info[2]+","+info[3]+","+info[4]+","+info[5]);
					}
					line = br.readLine();
				}
			}
		}
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int userId = Integer.parseInt(key.toString());
			String userInfo = map.get(userId);
			for (Text tuples : values) {
				context.write(new Text(userInfo), tuples);
			}
		}
	}
	
	

	public static void main(String[] args) throws Exception {
		Configuration conf1 = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf1, args).getRemainingArgs();
		if (otherArgs.length != 5) {
			System.err.println("Usage: <File with user and friends> <userdata.txt> <tempPath_job1> <tempPath_job2><output_Path>");
			System.exit(2);
		}
		conf1.set("Data", otherArgs[1]);
		Job job1 = Job.getInstance(conf1, "Minimum Age");

		job1.setJarByClass(minAge.class);
		job1.setMapperClass(MapClass_job1.class);
		job1.setReducerClass(Reducer_job1.class);

		FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job1, new Path(otherArgs[2]));

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		if (!job1.waitForCompletion(true)) {
			System.exit(1);
		}

		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "Decreasing order");

		job2.setJarByClass(minAge.class);
		job2.setMapperClass(MapClass_job2.class);
		job2.setReducerClass(Reducer_job2.class);

		FileInputFormat.addInputPath(job2, new Path(otherArgs[2]));
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[3]));

		job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);

		job2.setMapOutputKeyClass(LongWritable.class);
		job2.setMapOutputValueClass(Text.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		if (!job2.waitForCompletion(true)) {
			System.exit(1);
		}
		
		Configuration conf3 = new Configuration();
		conf3.set("Data", otherArgs[1]);
		Job job3 = Job.getInstance(conf3, "Decreasing order");

		job3.setJarByClass(minAge.class);
		job3.setMapperClass(MapClass_job3.class);
		job3.setReducerClass(Reducer_job3.class);

		FileInputFormat.addInputPath(job3, new Path(otherArgs[3]));
		FileOutputFormat.setOutputPath(job3, new Path(otherArgs[4]));

		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(Text.class);

		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);

		if (!job3.waitForCompletion(true)) {
			System.exit(1);
		}
	}
}