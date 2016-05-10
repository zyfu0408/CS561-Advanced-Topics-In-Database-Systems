package answer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Q4 extends Configured implements Tool{
	public static class Map extends Mapper<LongWritable, Text, IntWritable, Text>{
		
		private HashMap<String,String> cusInfo = new HashMap<String,String>();
        private IntWritable outPutKey = new IntWritable(); 
        private Text outPutValue = new Text(); 

        //@Override
		protected void setup(Context context) throws IOException{
			Path[] cachePaths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for(Path p:cachePaths){
				if(p.toString().endsWith("customers.txt")){
					BufferedReader br = new BufferedReader(new FileReader(p.toString())); 
					while(br.readLine() != null){
						String[] str = br.readLine().split(",",5);
						cusInfo.put(str[0], str[3]); // (CustomerID,CountryCode)
					}
				}
			}	
		}
	
		public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException{
			String line = value.toString();
			String[] lineSplit = line.split(",", 5);
			String cc = cusInfo.get(lineSplit[1]); // get CountryCode related to lineSplit[1](=CustomerID)
			//int count = 0;
			if(cc != null){
				//count ++;
				outPutKey.set(Integer.parseInt(cc));
				outPutValue.set(lineSplit[1]+","+lineSplit[2]); // (CustomerID,TransTotal)
				context.write(outPutKey, outPutValue);
			}
			//System.out.println("testTEST_map_count:"+count);
		}
	}
	
	public static class Reduce extends Reducer<IntWritable, Text, IntWritable, Text>{
		float minTT = Float.MAX_VALUE;
		float maxTT = Float.MIN_VALUE;
		String cid = null;
        private Text reduceOutputValue = new Text(); 
		
		public void reduce(IntWritable key, Iterable<Text> values, Context context)throws IOException, InterruptedException{
			HashMap<String,Boolean> uniqueCustomers = new HashMap<String,Boolean>();
			int num = 0;
			Iterator<Text> value = values.iterator();
			while(value.hasNext()){
				String line = value.next().toString();
				String[] joinInfo = line.split(",");
				cid = joinInfo[0];
				Boolean tag = uniqueCustomers.containsKey(cid);
				if(!tag) {
					num ++;
					uniqueCustomers.put(cid, new Boolean(true));
				}				
				if(Float.parseFloat(joinInfo[1]) < minTT){
					minTT = Float.parseFloat(joinInfo[1]);
				}
				if(Float.parseFloat(joinInfo[1]) > maxTT){
					maxTT = Float.parseFloat(joinInfo[1]);
				}
			}
	    	String minTT_format = new BigDecimal(Float.toString(minTT)).toPlainString();
	    	String maxTT_format = new BigDecimal(Float.toString(maxTT)).toPlainString();
	    	String outTrans = Integer.toString(num)+ "," + minTT_format + "," + maxTT_format;
			reduceOutputValue.set(outTrans);
			context.write(key, reduceOutputValue);
		}
	}
	
	public int run(String[] args) throws Exception {
		Job job = new Job();
		Configuration conf = job.getConfiguration();   
		DistributedCache.addCacheFile(new Path(args[0]).toUri(), conf);  // HDFS path
	   
	    job.setJobName("Query4");
	    job.setJarByClass(Q4.class);
			
		     
	    job.setMapOutputKeyClass(IntWritable.class);
	    job.setMapOutputValueClass(Text.class);
	     
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(Text.class);
	
	    job.setMapperClass(Map.class);
	    job.setReducerClass(Reduce.class);
	     
        job.setInputFormatClass(TextInputFormat.class);   
        job.setOutputFormatClass(TextOutputFormat.class);
	
	    FileInputFormat.addInputPath(job, new Path(args[1]));
	    FileOutputFormat.setOutputPath(job, new Path(args[2]));
	    job.waitForCompletion(true);
	    return job.isSuccessful()?0:1;
		
	}

	public static void main(String[] args) throws Exception {
		int returnCode =  ToolRunner.run(new Q4(),args); 
	}

}


