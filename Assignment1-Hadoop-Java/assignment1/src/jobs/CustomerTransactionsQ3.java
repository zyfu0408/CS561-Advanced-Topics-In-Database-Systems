package jobs;

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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;

@SuppressWarnings("deprecation")
public class CustomerTransactionsQ3 {
	
	public static class CustomerTransMapper
	extends Mapper<LongWritable, Text, IntWritable, Text>{
		private HashMap<Integer, String> customerKey = new HashMap<Integer, String>();
		IntWritable outputKey = new IntWritable();

		private void readCustomersDetailsFile(String line) throws IOException{
			String[] customerDetails = line.split(",");
	        int customerId = Integer.parseInt(customerDetails[0]); 
	        String otherDetails = customerDetails[1] + "," + customerDetails[4];
	        customerKey.put(customerId, otherDetails);
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
		        while (customerRecLine!= null && !"".equals(customerRecLine)) {
		        	customerRecLine = bufferedReader.readLine();
		        	if(customerRecLine!=null){
		        		readCustomersDetailsFile(customerRecLine);
		        	}
		        }
	        }
	    }
		
		public void map(LongWritable key, Text value, Context context
				)throws IOException, InterruptedException{

			String outputValue = "";
			
			String[] transaction = value.toString().split(",");
			
			int customerId =  Integer.parseInt(transaction[1]);
			int numOfTransactions = 1;
			int itemCount = Integer.parseInt(transaction[3]);
			float transAmount = Float.parseFloat(transaction[2]);
			
			String customerOtherDetails = customerKey.get(customerId);
						
			outputKey.set(customerId);
			
			outputValue += customerOtherDetails + "," + numOfTransactions + "," + transAmount + "," + itemCount;
			
			context.write(outputKey, new Text(outputValue));
		}
	}
	
	private static class CustomerTransReducer
	extends Reducer<IntWritable, Text, Text, Text>{
		
		public void reduce(IntWritable key, Iterable<Text> values, Context context
				)throws IOException, InterruptedException{
			
			int numOfTrans = 0;
			float totalSum = 0f;
			
			int minItems = Integer.MAX_VALUE; 
			String customerInfo[] = null;
			
			for(Text value : values)
			{
				customerInfo = value.toString().split(",");
				totalSum += Float.parseFloat(customerInfo[3]); 
				numOfTrans += 1;
				if (minItems > Integer.parseInt(customerInfo[4])){
					minItems = Integer.parseInt(customerInfo[4]);
				}
				

			}
			
			if(customerInfo != null){
				String outputString = customerInfo[0] + "," + customerInfo[1] + "," + numOfTrans + "," + totalSum + "," + minItems;
				context.write(null, new Text( key.toString() + "," + outputString));
			}
			
		}
	}
	

	public static void main(String[] args) throws Exception{
		 Configuration conf = new Configuration();
		 DistributedCache.addCacheFile(new URI(args[0]), conf);
		 Job job = new Job(conf, "CustomerTransactionsQ3");
		 job.setJarByClass(CustomerTransactionsQ3.class);

		 FileInputFormat.addInputPath(job, new Path(args[1]));
		 FileOutputFormat.setOutputPath(job, new Path(args[2]));
		 
		 job.setMapperClass(CustomerTransMapper.class);
		 job.setMapOutputKeyClass(IntWritable.class);
		 
		 job.setReducerClass(CustomerTransReducer.class);
		 
		 job.setOutputKeyClass(IntWritable.class);
		 job.setOutputValueClass(Text.class);

		 long start = System.currentTimeMillis();
		 job.waitForCompletion(true);
		 long end = System.currentTimeMillis();
		 System.out.println((end-start)/1000.0);
	}

}