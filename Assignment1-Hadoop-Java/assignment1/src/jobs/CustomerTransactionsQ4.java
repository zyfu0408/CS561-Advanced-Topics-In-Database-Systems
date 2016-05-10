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
public class CustomerTransactionsQ4 {
	
	public static class JoinMapper
	extends Mapper<LongWritable, Text, IntWritable, Text>{
		private HashMap<Integer, Integer> customerKey = new HashMap<Integer, Integer>();
		
		private void readCustomersDetailsFile(String line) throws IOException{
			String[] customerDetails = line.split(",");
	        int customerId = Integer.parseInt(customerDetails[0]); 
	        int countryCode = Integer.parseInt(customerDetails[3]);
	        customerKey.put(customerId, countryCode);
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
			float transAmount = Float.parseFloat(transaction[2]);
			int numOfCustomer = 1;
			
			int countryCode = customerKey.get(customerId);
						
			outputKey.set(countryCode);
			
			
			outputValue += countryCode + "," + numOfCustomer + "," + transAmount;
			
			context.write(outputKey, new Text(outputValue));
		}
	}
	
	private static class ContryCodeReducer
	extends Reducer<IntWritable, Text, IntWritable, Text>{
		
		public void reduce(IntWritable key, Iterable<Text> values, Context context
				)throws IOException, InterruptedException{
			
			float minTransTotal = Float.MAX_VALUE; 
			float maxTransTotal = 0; 
			int numOfCustomers = 0;
			
			for(Text value : values)
			{
				String[] countryRecord = value.toString().split(",");
				
				numOfCustomers += Integer.parseInt(countryRecord[1]);
				
				float transTotal = Float.parseFloat(countryRecord[2]);
				
				if(minTransTotal > transTotal)
					minTransTotal = transTotal;
				
				if(maxTransTotal < transTotal)
					maxTransTotal = transTotal;
			}
			
			String outValue = key.get() + "," + numOfCustomers + "," + minTransTotal + "," + maxTransTotal;
			
			context.write(null, new Text(outValue));
			
		}
	}
	

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		 Configuration conf = new Configuration();
		 DistributedCache.addCacheFile(new URI(args[0]), conf);
		 Job job = new Job(conf, "CustomerTransactionsQ4");
		 job.setJarByClass(CustomerTransactionsQ4.class);

		 FileInputFormat.addInputPath(job, new Path(args[1]));
		 FileOutputFormat.setOutputPath(job, new Path(args[2]));
		 
		 job.setMapperClass(JoinMapper.class);
		 job.setMapOutputKeyClass(IntWritable.class);
		 
		 job.setReducerClass(ContryCodeReducer.class);
		 
		 job.setOutputKeyClass(IntWritable.class);
		 job.setOutputValueClass(Text.class);

		 long start = System.currentTimeMillis();
		 job.waitForCompletion(true);
		 long end = System.currentTimeMillis();
		 System.out.println((end-start)/1000.0);

	}

}