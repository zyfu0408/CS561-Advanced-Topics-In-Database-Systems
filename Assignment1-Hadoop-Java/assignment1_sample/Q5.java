package answer;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q5 {
	
	public static class CustomerMapper
	extends Mapper<LongWritable, Text, IntWritable, Text>{
		private HashMap<Integer, String> customerKey = new HashMap<Integer, String>();
		
		private void readCustomersDetailsFile(String line) throws IOException{
			String[] customerDetails = line.split(",");
	        int customerId = Integer.parseInt(customerDetails[0]); 
	        String customerName = customerDetails[1];
	        customerKey.put(customerId, customerName);
		}
		
		@Override
		public void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			URI[] uris = DistributedCache.getCacheFiles(conf);
			
	        if(uris!=null){
		        FSDataInputStream customerData = FileSystem.get(context.getConfiguration()).open(new Path(uris[0]));
		        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(customerData));
		        
		        String customerRecLine = bufferedReader.readLine();
		        readCustomersDetailsFile(customerRecLine);
		        while (customerRecLine!= null&&!"".equals(customerRecLine)) {
		        	customerRecLine = bufferedReader.readLine();
		        	if(customerRecLine!=null){
		        		readCustomersDetailsFile(customerRecLine);
		        	}
		        }
	        }
	    }
		IntWritable outputKey = new IntWritable();
		public void map(LongWritable key, Text value, Context context
				)throws IOException, InterruptedException{

			String outputValue = "";
			
			String[] transaction = value.toString().split(",");
			
			int customerId =  Integer.parseInt(transaction[1]);
			int numOfTransactions = 1;
			
			String customerName = customerKey.get(customerId);
						
			outputKey.set(customerId);
			
			
			outputValue += customerName + "," + numOfTransactions;
			
			context.write(outputKey, new Text(outputValue));
		}
	}
	
	private static class CustomerNameReducer
	extends Reducer<IntWritable, Text, Text, IntWritable>{
		int localmin=10000;
		public void reduce(IntWritable key, Iterable<Text> values, Context context
				)throws IOException, InterruptedException{
			
			int numOfTrans = 0;
			String customerName = "";

			for(Text value : values)
			{
				customerName = value.toString().split(",")[0];
				numOfTrans += 1;

			}
			if (numOfTrans < localmin){			
			context.write(new Text(customerName), new IntWritable( numOfTrans));
			localmin = numOfTrans;
			}
			
		}
	}
	

	public static class MinTransMapper
	extends Mapper<LongWritable, Text, Text, Text>{
		public void map(LongWritable key, Text value, Context context
				)throws IOException, InterruptedException{	
			String filename = ((FileSplit) context.getInputSplit()).getPath().toString();
			if (filename.charAt(0)!='_'){
			context.write(new Text("customers"), value);}
		}
	}
	
	private static class MinTransReducer
	extends Reducer<Text, Text, Text, Text>{
		
		public void reduce(Text key, Iterable<Text> values, Context context
				)throws IOException, InterruptedException{
			
			int minTrans = Integer.MAX_VALUE;
			String customerNames = "";

			for(Text value : values)
			{
				String[] custTransCounts = value.toString().split("\\t");
				int transCount = Integer.parseInt(custTransCounts[1]);
				
				if (transCount < minTrans ){
					minTrans = transCount;
					customerNames = "";
				}else if (transCount == minTrans){
					customerNames += custTransCounts[0] + "," + transCount + "\n"; 
				}
			}
			
			context.write(null, new Text( customerNames));
			
		}
	}
	
	
	public static void main(String[] args) throws Exception{
		 Configuration conf = new Configuration();
		 DistributedCache.addCacheFile(new URI(args[0]), conf);
		 Job job = new Job(conf, "Q5_job1");
		 job.setJarByClass(Q5.class);

		 FileInputFormat.addInputPath(job, new Path(args[1]));
		 FileOutputFormat.setOutputPath(job, new Path(args[2]));
		 
		 job.setMapperClass(CustomerMapper.class);
		 job.setMapOutputKeyClass(IntWritable.class);
		 
		 job.setReducerClass(CustomerNameReducer.class);
		 
		 job.setOutputKeyClass(IntWritable.class);
		 job.setOutputValueClass(Text.class);

		 job.waitForCompletion(true);
		 
		 Job job2 = new Job(conf, "Q5_job2");
		 job2.setJarByClass(Q5.class);

		 FileInputFormat.addInputPath(job2, new Path(args[2]));
		 FileOutputFormat.setOutputPath(job2, new Path(args[3]));
		 
		 job2.setMapperClass(MinTransMapper.class);
		 job2.setMapOutputKeyClass(Text.class);
		 
		 job2.setReducerClass(MinTransReducer.class);
		 job2.setNumReduceTasks(1);
		 job2.setOutputKeyClass(Text.class);
		 job2.setOutputValueClass(Text.class);

		 
		 job2.waitForCompletion(true);
		 
	}

}
