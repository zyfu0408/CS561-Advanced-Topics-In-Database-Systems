package jobs;

import java.io.IOException;
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


public class GetCustomerTransactionsQ2 {
	public static class CustomerTransactionsMapper
	extends Mapper<LongWritable, Text, IntWritable, Text>{

		IntWritable outputKey = new IntWritable();
		public void map(LongWritable key, Text value, Context context
				)throws IOException, InterruptedException{

			String outputValue = "";
			
			String[] transaction = value.toString().split(",");
			
			int customerId =  Integer.parseInt(transaction[1]);
			float transAmount = Float.parseFloat(transaction[2]);
			int numOfTransactions = 1;
						
			outputKey.set(customerId);
			
			outputValue += transAmount + "," + numOfTransactions;
			
			context.write(outputKey, new Text(outputValue));
		}
	}
	
	private static class CustomerTransactionsReducer
	extends Reducer<IntWritable, Text, IntWritable, Text>{
		
		public void reduce(IntWritable key, Iterable<Text> values, Context context
				)throws IOException, InterruptedException{
			
			float totalTransAmounts = 0f; 
			int numberOfTrans = 0;
			
			for(Text value : values)
			{
				String[] transRecord = value.toString().split(",");
				
				numberOfTrans += 1;
				
				totalTransAmounts += Float.parseFloat(transRecord[0]);
			}
			
			String outValue = key.get() + "," + numberOfTrans + "," + totalTransAmounts;
			
			context.write(key, new Text(outValue));
			
		}
	}

   public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "transaction information");
	    job.setJarByClass(GetCustomerTransactionsQ2.class);
	    job.setMapperClass(CustomerTransactionsMapper.class);
	    //job.setCombinerClass(CustomerTransactionsReducer.class);
	    job.setReducerClass(CustomerTransactionsReducer.class);
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    long start = System.currentTimeMillis();
		job.waitForCompletion(true);
		long end = System.currentTimeMillis();
		System.out.println((end-start)/1000.0);
   }
}
