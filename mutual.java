import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

/**
 * Created by ram on 10/1/16.
 */
class user{
	String userId;
	String name;
	String city;
	
}
public class mutual {

	
	
    public static class Map
            extends Mapper<LongWritable, Text, Text, Text> {

        Text user = new Text();
        Text friends = new Text();
        
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {

        	//Configuration conf=context.getConfiguration();
        	//final String f1=conf.get("friend1");
        	//final String f2=conf.get("friend2");
        	HashMap<String,user> hm=new HashMap<String,user>();
        	Path pt=new Path("/user/shilpa/input/userdata.txt");
        	FileSystem fs = FileSystem.get(context.getConfiguration());
        	BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
        	try {
        	  String line;
        	  line=br.readLine();
        	  while (line != null){
        	    String[] fields=line.split(",");
        	    user u1=new user();
        	    u1.userId=fields[0];
        	    u1.name=fields[1];
        	    u1.city=fields[4];
        	    hm.put(fields[0],u1 );
        	    // be sure to read the next line otherwise you'll get an infinite loop
        	    line = br.readLine();
        	  }
        	  //System.out.println(hm.get("0").name);
        	}
        	catch (IOException e) {
        		   e.printStackTrace();
        		  }
        	finally {
        	  // you should close out the BufferedReader
        	  br.close();
        	}
        	
            //split the line into user and friends
            String[] split = value.toString().split("\\t");
            //split[0] - user
            //split[1] - friendList
            String userid = split[0];
          //  if(split[0].equalsIgnoreCase(f1) || split[0].equalsIgnoreCase(f2) )
            //{	
            	if( split.length == 1 ) {
            		return;
            	}
            	String[] values=split[1].split(",");
            	for (int i=0;i<values.length;i++)
            	{
            		user u2=new user();
            		u2=hm.get(values[i].toString().trim());
            		values[i]=u2.name+":"+u2.city;
            	//System.out.println(u2.name);
            	}
            //String friends_final=values.;
            	String friends_final = String.join(",", values);
            	String[] others = split[1].split(",");
            //String friends_final="";
            	for( String friend : others ) {
            	
            	
                if( userid.equals(friend) )
                    continue;
                //user u2=new user();
            	//u2=hm.get(friend);
                String userKey = (Integer.parseInt(userid) < Integer.parseInt(friend) ) ? userid + "," + friend : friend + "," + userid;
                String regex = "((\\b" + friend + "[^\\w]+)|\\b,?" + friend + "$)";
                //friends_final.concat(u2.userId+":"+u2.name+":"+u2.city);
                
                friends.set(friends_final.replaceAll(regex,""));
                user.set(userKey);
                context.write(user, new Text(friends_final));
                
            	}
            
            }
        //}

    }
    public static class Reduce
            extends Reducer<Text,Text,Text,Text> {

        private String findMatchingFriends( String list1, String list2 ) {

            if( list1 == null || list2 == null )
                return null;

            String[] friendsList1 = list1.split(",");
            String[] friendsList2 = list2.split(",");

            //use LinkedHashSet to retain the sort order
            LinkedHashSet<String> set1 = new LinkedHashSet<>();
            for( String user: friendsList1 ) {
                set1.add(user);
            }

            LinkedHashSet<String> set2 = new LinkedHashSet<>();
            for( String user: friendsList2 ) {
                set2.add(user);
            }

            //keep only the matching items in set1
            set1.retainAll(set2);
            
            return set1.toString();
        }

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            String[] friendsList = new String[2];
            int i = 0;

            for( Text value: values ) {
                friendsList[i++] = value.toString();
            }
            String mutualFriends = findMatchingFriends(friendsList[0],friendsList[1]);
            if( mutualFriends != null && mutualFriends.length() != 0 ) {
                
            	context.write(key, new Text( mutualFriends ) );
            }
        }

    }
    // Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
// get all args
        

// create a job with name "MutualFriends"
        Job job = new Job(conf, "Mutual");
        //conf.set("friend1", otherArgs[2]);
        //conf.set("friend2", otherArgs[3]);
        job.setJarByClass(mutual.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
// uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);
// set output key type
        job.setOutputKeyClass(Text.class);
// set output value type
        job.setOutputValueClass(Text.class);
//set the HDFS path of the input data
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        //FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
// set the HDFS path for the output
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
//Wait till job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}