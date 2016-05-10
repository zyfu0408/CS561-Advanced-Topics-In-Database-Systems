package question2;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
//import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;

import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
//import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;

public class GetSalaryAggregates {

	public static class SalaryAggregatesMapper implements Mapper<LongWritable, Text, Text, Text>{

		@Override
		public void configure(JobConf arg0) {
			// TODO Auto-generated method stub
			
		}
		@Override
		public void close() throws IOException {
			// TODO Auto-generated method stub
			
		}
		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> collector, Reporter reporter)
				throws IOException {
			String outputValue = "";
			
			String[] customer = value.toString().split(",");
			String salary = customer[3];
			String gender = customer[4];
			
			int numberOfGender = 1;
			
			outputValue += numberOfGender;
			
			collector.collect(new Text(salary + "," + gender), new Text(outputValue));			
		}
	}
	
	private static class SalaryAggregatesReducer implements Reducer<Text, Text, Text, Text>{

		@Override
		public void configure(JobConf arg0) {
			
		}

		@Override
		public void close() throws IOException {
			
		}

		@Override
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> outCollector,
				Reporter arg3) throws IOException {
			
			int genderCount = 0;
			while(values.hasNext()){
				values.next();
				genderCount += 1;
			}
			outCollector.collect(null, new Text(key.toString() + "," + genderCount));	
			
		}
	}

   public static void main(String[] args) throws Exception {
	   JobClient client = new JobClient(); 
	   JobConf conf = new JobConf();
	   conf.setOutputKeyClass(Text.class);
	   
	   conf.setOutputValueClass(Text.class);
	   
	   FileInputFormat.setInputPaths(conf, new Path(args[0]));
	   
	   Path outPath = new Path(args[1]);
	   
	   FileOutputFormat.setOutputPath(conf, outPath);
	   
	   conf.setMapperClass( SalaryAggregatesMapper.class);
	   conf.setInputFormat(CustInputFormat.class);
	   conf.setJarByClass(GetSalaryAggregates.class);
	   conf.set("jsoninput.start", "{");
	   conf.set("jsoninput.end", "}");

	   conf.setReducerClass( SalaryAggregatesReducer.class);
	   conf.setNumReduceTasks(10);
	   
	   conf.setOutputFormat(TextOutputFormat.class);
	   
	   FileSystem dfs = FileSystem.get(outPath.toUri(), conf);
	   
	   if(dfs.exists(outPath)){
		   dfs.delete(outPath, true);
	   }
	   
	   client.setConf(conf);
	   
	   JobClient.runJob(conf);
  }
}
