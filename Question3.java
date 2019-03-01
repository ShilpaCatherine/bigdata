import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Question3 {
	

	public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

		static HashMap<Integer, String> map = new HashMap<Integer, String>();

		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			Configuration conf = context.getConfiguration();
			Path part = new Path(context.getConfiguration().get("Data"));// Location of file in HDFS
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
						map.put(Integer.parseInt(info[0]), info[1] + ":" + info[5]);
					}
					line = br.readLine();
				}
			}
		}

		public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
			Configuration conf_1 = context.getConfiguration();
			int input_friend1 = Integer.parseInt(conf_1.get("InputFriend1"));
			int input_friend2 = Integer.parseInt(conf_1.get("InputFriend2"));
			String[] friends = values.toString().split("\t");
			if (friends.length == 2) {
				int friend1ID = Integer.parseInt(friends[0]);
				String[] friendsList = friends[1].split(",");
				int friend2ID;
				StringBuilder sb;;
				Text tuple_key = new Text();
				for (String friend2 : friendsList) {					
					friend2ID = Integer.parseInt(friend2);
					if ((friend1ID == input_friend1 && friend2ID == input_friend2)
							|| (friend1ID == input_friend2 && friend2ID == input_friend1)) {
						sb = new StringBuilder();
						if (friend1ID < friend2ID) {
							tuple_key.set(friend1ID + "," + friend2ID);
						} else {
							tuple_key.set(friend2ID + "," + friend1ID);
						}
						for (String friendInfo : friendsList) {
							int friendInfo2 = Integer.parseInt(friendInfo);
							sb.append(friendInfo2+":"+map.get(friendInfo2) + ",");
						}
						if (sb.length() > 0) {
							sb.deleteCharAt(sb.length() - 1);
						}
						context.write(tuple_key, new Text(sb.toString()));
					}
				}
			}
		}
	}

	public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			HashMap<String, Integer> friendsHashMap = new HashMap<String, Integer>();
			StringBuilder commonFriendLine = new StringBuilder();
			Text commonFriend = new Text();
			for (Text tuples : values) {
				String[] friendsList = tuples.toString().split(",");
				for (String eachFriend : friendsList) {
					if (friendsHashMap.containsKey(eachFriend)) {
						String[] info = eachFriend.split(":");
						commonFriendLine.append(info[1]+":"+info[2]+ ",");
					} else {
						friendsHashMap.put(eachFriend, 1);
					}
				}
			}
			if (commonFriendLine.length() > 0) {
				commonFriendLine.deleteCharAt(commonFriendLine.length() - 1);
			}
			commonFriend.set(new Text(commonFriendLine.toString()));
			context.write(key, commonFriend);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf1 = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf1, args).getRemainingArgs();
		if (otherArgs.length != 5) {
			System.out.println(
					"Usage: <Common friends input file path> <user_data file path> <output_path> <User-ID1> <User-ID2> ");
			System.exit(1);
		}

		conf1.set("InputFriend1", otherArgs[3]);
		conf1.set("InputFriend2", otherArgs[4]);
		conf1.set("Data", otherArgs[1]);

		Job job1 = Job.getInstance(conf1, "Mutual-Friends of userA and userB");

		job1.setJarByClass(Question3.class);
		job1.setMapperClass(MapperClass.class);
		job1.setReducerClass(ReducerClass.class);

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job1, new Path(otherArgs[2]));

		if (!job1.waitForCompletion(true)) {
			System.exit(1);
		}
	}

}