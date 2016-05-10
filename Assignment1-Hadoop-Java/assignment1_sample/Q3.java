package answer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.lang.Math;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
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




public class Q3 extends Configured implements Tool{
	public static class Map extends Mapper<LongWritable, Text, IntWritable, Text>{
		 
		private HashMap<String,String> cusInfo = new HashMap<String,String>();
        private IntWritable outPutKey = new IntWritable();   
        private Text outPutValue = new Text(); 
		
		protected void setup(Context context) throws IOException{
			Path[] cachePaths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for(Path p:cachePaths){
				if(p.toString().endsWith("customers.txt")){
					BufferedReader br = new BufferedReader(new FileReader(p.toString())); 
					while(br.readLine() != null){
						String[] str = br.readLine().split(",",5);
						cusInfo.put(str[0], str[1]+","+str[4]);
					}
				}
			}	
		}
	
		public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException{
			
			String line = value.toString();
			String []lineSplit = line.split(",", 5);
			String info = cusInfo.get(lineSplit[1]);
			if(info != null){
				outPutKey.set(Integer.parseInt(lineSplit[1]));
				outPutValue.set(info + ","+ lineSplit[2]+","+lineSplit[3]);
				context.write(outPutKey, outPutValue);
			}

		}
	
	}
	
	public static class Reduce extends Reducer<IntWritable, Text, IntWritable, Text>{
		int minItem = Integer.MAX_VALUE;
		int num = 0;
		double sum = 0;
		String name = null;
		String salary = null;
        
        private Text outPutValue1 = new Text(); 
		
		public void reduce(IntWritable key, Iterable<Text> values, Context context)throws IOException, InterruptedException{
			
			Iterator<Text> value = values.iterator();
			while(value.hasNext()){
				String line = value.next().toString();
				String[] joinInfo = line.split(",");
				name = joinInfo[0];
				salary = joinInfo[1];
				num ++;
				sum += Double.parseDouble(joinInfo[2]);
				if(Integer.parseInt(joinInfo[3]) < minItem){
					minItem = Integer.parseInt(joinInfo[3]);
				}
			}

	    	String sum_format = new BigDecimal(Double.toString(Math.round(sum*100.0)/100.0)).toPlainString();
			String outTrans = name + " " + salary + " " + Integer.toString(num)+ " " +sum_format +" "+Integer.toString(minItem);
			
			outPutValue1.set(outTrans);
			
			context.write(key, outPutValue1);

		}
	}
	
	public int run(String[] args) throws Exception {
		
		Job job = new Job();
		Configuration conf = job.getConfiguration(); 
		DistributedCache.addCacheFile(new Path(args[0]).toUri(), conf);  
	   
	    job.setJobName("Query3");
	    job.setJarByClass(Q3.class);
			
		     
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
		int returnCode =  ToolRunner.run(new Q3(),args); 

	}

}
