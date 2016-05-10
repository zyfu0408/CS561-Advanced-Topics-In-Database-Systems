package assn4.problem2;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.lang.Math;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit; 
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Q2 extends Configured implements Tool{
	
	public static class CustomerInputFormat extends FileInputFormat<LongWritable, Text> {
		
		public CustomerInputFormat(){}
		
		@Override 
		public RecordReader<LongWritable, Text> 
				createRecordReader(InputSplit split, TaskAttemptContext context) {
			return new JsonRecordReader();
		}
		
		/**
		 * This class uses the LineRecordReader to read JSON and return it as a Text object. 
		 */
		public class JsonRecordReader extends RecordReader<LongWritable, Text> {
			
//			private static final Log LOG = LogFactory.getLog(LineRecordReader.class);

			private CompressionCodecFactory compressionCodecs = null;
			private long start;
			private long pos;
			private long end;
			private LineReader in;
			private int maxLineLength;
			private LongWritable key = null;
			private Text value = null;
			private Text outvalue = null;
			StringBuffer tmp = new StringBuffer("");
			
			public void initialize(InputSplit genericSplit,TaskAttemptContext context) 
					throws IOException {
			     
				FileSplit split = (FileSplit) genericSplit;
				Configuration job = context.getConfiguration();
				this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength",
												Integer.MAX_VALUE);
				start = split.getStart();
				end = start + split.getLength();
				final Path file = split.getPath();
				compressionCodecs = new CompressionCodecFactory(job);
				final CompressionCodec codec = compressionCodecs.getCodec(file);

				// open the file and seek to the start of the split
				FileSystem fs = file.getFileSystem(job);
				FSDataInputStream fileIn = fs.open(split.getPath());
				boolean skipFirstLine = false;
				if (codec != null) {
					in = new LineReader(codec.createInputStream(fileIn), job);
					end = Long.MAX_VALUE;
				} else {
					if (start != 0) {
						skipFirstLine = true;
						--start;
						fileIn.seek(start);
					}
					in = new LineReader(fileIn, job);
				}
				if (skipFirstLine) {  // skip first line and re-establish "start".
					start += in.readLine(new Text(), 0,
							(int)Math.min((long)Integer.MAX_VALUE, end - start));
				}
				this.pos = start;
			}			


			public boolean nextKeyValue() throws IOException {
							
				if (key == null) {
					key = new LongWritable();
				}
				key.set(pos);
				if (value == null) {
					value = new Text();
				}
				int newSize = 0;
				String str = "";
//				StringBuffer tmp = new StringBuffer("");
				
				while (pos < end) {
					
					newSize = in.readLine(value, maxLineLength,
											Math.max((int)Math.min(Integer.MAX_VALUE, end-pos),
																	maxLineLength));
					if (newSize == 0)
						break;
					pos += newSize;
					str = value.toString().trim();
					
//					outvalue = new Text(str);
//					tag = true;

					if (str.contains("{")) {
						outvalue = new Text("");
						break;
					} else if (str.contains("}")) {
						outvalue = new Text(tmp.toString());
						tmp.setLength(0);
					} else {
						String[] parts = str.split(":");
						tmp.append(parts[1]);
						outvalue = new Text("");
					}					
					
					if (newSize < maxLineLength) {
						break;
					}
					// line too long. try again
//					LOG.info("Skipped line of size " + newSize + " at pos " + (pos - newSize));
				}
				
				if (newSize == 0) {
					key = null;
					value = null;
					return false;
				} else {
					return true;
				}
			  
			}

/*			public boolean nextKeyValue() throws IOException {
				if (key == null) {
					key = new LongWritable();
				}
				key.set(pos);
				if (value == null) {
					value = new Text();
				}
				int newSize = 0;
				while (pos < end) {
					newSize = in.readLine(
							value,
							maxLineLength,
							Math.max((int) Math.min(Integer.MAX_VALUE, end
									- pos), maxLineLength));
					if (newSize == 0) {
						break;
					}
					pos += newSize;
					if (newSize < maxLineLength) {
						break;
					}

					// line too long. try again
//					LOG.info("Skipped line of size " + newSize + " at pos " + (pos - newSize));
				}
				if (newSize == 0) {
					key = null;
					value = null;
					return false;
				} else {
					return true;
				}
			}			
*/			
			@Override
			public LongWritable getCurrentKey() {
				return key;
			}

			
			@Override
			public Text getCurrentValue() {
				String[] parts = outvalue.toString().split(",");
				if (parts.length == 5) {
					return outvalue;
				}
				else {
					return new Text("");
				}
				
			}

			  
			public float getProgress() {
				if (start == end) {
					return 0.0f;
				} else {
					return Math.min(1.0f, (pos - start) / (float)(end - start));
				}
			}

			
			public synchronized void close() throws IOException {
				if (in != null) {
					in.close(); 
				}
			}
			
			
			
		}
		
	}
	
	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
	
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			Text outPutKey = new Text();
			Text outPutValue = new Text();
			String salary = "";
			String gender = "female";
			
			if ( !value.toString().isEmpty() ) {
				String line = value.toString();
				String []lineSplit = line.split(",");
				salary = lineSplit[3];
				gender = lineSplit[4];
				outPutKey.set(salary);
				outPutValue.set(gender);
				context.write(outPutKey, outPutValue);				
			}


		}
	}
	
	public static class Reduce extends Reducer<Text, Text, Text, Text>{
		
		Text outvalueF = new Text("");
		Text outvalueM = new Text("");
//		Text test = new Text("");
		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException{

			int cF = 0;
			int cM = 0;
			String line = "";
			
			Iterator<Text> value = values.iterator();
			while(value.hasNext()) {
				line = value.next().toString().trim();
				if (line.contains("female")){
					cF ++; 
				}
				else {
					cM ++;
				}
			}
			
			outvalueF.set("female: " + Integer.toString(cF));
			outvalueM.set("male: " + Integer.toString(cM));
			context.write(key, outvalueF);
			context.write(key, outvalueM);

			
		}
	}
	
	public int run(String[] args) throws Exception {
		Job job = new Job();
		Configuration conf = job.getConfiguration();   
	   
	    job.setJobName("Q2");
	    job.setJarByClass(Q2.class);
			
		     
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	     
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	
	    job.setMapperClass(Map.class);
	    job.setReducerClass(Reduce.class);
	     
        job.setInputFormatClass(CustomerInputFormat.class);   
        job.setOutputFormatClass(TextOutputFormat.class);
	
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    job.waitForCompletion(true);
	    return job.isSuccessful()?0:1;
		
	}

	public static void main(String[] args) throws Exception {
		int returnCode =  ToolRunner.run(new Q2(),args); 
	}

}

