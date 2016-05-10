package jobs;

import org.apache.hadoop.*;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

@SuppressWarnings("unused")
public class GetCustomersQ1 {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> { 
    	private Text outString = new Text();
    	private Text custKey = new Text();
    	 	
    	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
    		String line = value.toString();
    		String[] record = line.split(",");
    		
    		if(Integer.parseInt(record[3]) >= 2 && Integer.parseInt(record[3])<= 6 ){
    			custKey.set(record[0]);
    			outString = new Text(record[3]);
	    		output.collect(custKey, outString);
    		}
    	}
    }
    
	public static void main(String[] args) throws IOException {
	     JobConf conf = new JobConf(GetCustomersQ1.class);
 	     conf.setJobName("GetCustomers");
 	
 	     conf.setOutputKeyClass(Text.class);
 	     conf.setOutputValueClass(Text.class);
 	
 	     conf.setMapperClass(Map.class);
 	     //conf.setCombinerClass(Reduce.class);
 	     //conf.setReducerClass(Reduce.class);
 	
 	     conf.setInputFormat(TextInputFormat.class);
 	     conf.setOutputFormat(TextOutputFormat.class);
 	
 	     FileInputFormat.setInputPaths(conf, new Path(args[0]));
 	     FileOutputFormat.setOutputPath(conf, new Path(args[1]));
 	
 	     JobClient.runJob(conf);
	}

}
