package answer;

import java.io.IOException;
import java.math.BigDecimal;
import java.lang.Math;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;


public class Q2 {
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, DoubleWritable> {
		private IntWritable cid = new IntWritable();
		private DoubleWritable transTotal = new DoubleWritable();

		public void map(LongWritable key, Text value,OutputCollector<IntWritable, DoubleWritable> output, 
				Reporter reporter) throws IOException {
			String line = value.toString();
			String[] parts = line.split(",");
			int cus_id = Integer.parseInt(parts[1]);
			double tran_total = Double.parseDouble(parts[2]);//

			cid = new IntWritable(cus_id);
			transTotal = new DoubleWritable(tran_total);
			output.collect(cid, transTotal);
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<IntWritable, DoubleWritable, IntWritable, Text> {
		private Text tranInfo = new Text();
		
		public void reduce(IntWritable key, Iterator<DoubleWritable> values,OutputCollector<IntWritable, Text> output, 
				Reporter reporter)throws IOException {

				int numTrans = 0;
				double sumTrans = 0;

				while (values.hasNext()) {
					numTrans += 1;
					sumTrans += values.next().get();
				}
		    	String sum_format = new BigDecimal(Double.toString(Math.round(sumTrans*100.0)/100.0)).toPlainString();
				String outTrans = Integer.toString(numTrans) + " " +sum_format;
				tranInfo = new Text(outTrans);
				output.collect(key, tranInfo);
			}
	}



	public static void main(String[] args) throws Exception{
	     JobConf conf = new JobConf(Q2.class);
	     conf.setJobName("Query2");
	     conf.setJarByClass(Q2.class);
	     
	     conf.setMapOutputKeyClass(IntWritable.class);
	     conf.setMapOutputValueClass(DoubleWritable.class);
	     
	     conf.setOutputKeyClass(IntWritable.class);
	     conf.setOutputValueClass(Text.class);
	
	     conf.setMapperClass(Map.class);
	     //conf.setCombinerClass(Reduce.class);
	     conf.setReducerClass(Reduce.class);
	     
	     conf.setInputFormat(TextInputFormat.class);
	     conf.setOutputFormat(TextOutputFormat.class);
	
	     FileInputFormat.setInputPaths(conf, new Path(args[0]));
	     FileOutputFormat.setOutputPath(conf, new Path(args[1]));
	
	     JobClient.runJob(conf);
	}

}