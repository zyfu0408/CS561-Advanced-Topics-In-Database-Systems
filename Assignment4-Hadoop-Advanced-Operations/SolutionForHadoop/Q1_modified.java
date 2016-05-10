package assn4.problem2;


import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Q1 extends Configured implements Tool {
	
	
	public static String windowLable(int X, int Y){
		int lableX = X/10;
		int lableY = Y/10;
		return Integer.toString(lableX)+","+Integer.toString(lableY);
	}

	public static ArrayList<String> customerWindow(int cX1, int cY1, int cX2, int cY2){
		ArrayList<String> wList = new ArrayList<String>();
		
		int wXmin = Math.min(cX1, cX2);
		int wXmax = Math.max(cX1, cX2);
		
		int wYmin = Math.min(cY1, cY2);
		int wYmax = Math.max(cY1, cY2);
		
		String topLeft = windowLable(wXmin,wYmax);
		String bottomRight = windowLable(wXmax,wYmin);
		
		int imin = Integer.parseInt(topLeft.split(",")[0]);
		int imax = Integer.parseInt(bottomRight.split(",")[0]);
		
		int jmin = Integer.parseInt(bottomRight.split(",")[1]);
		int jmax = Integer.parseInt(topLeft.split(",")[1]);

		for (int i = imin; i <= imax; i++) {
			for (int j = jmin; j <= jmax; j++) {
				wList.add(Integer.toString(i)+","+Integer.toString(j));
			}
		}
		return wList;
	}
	
	public static class MapP extends Mapper<LongWritable, Text, Text, Text>{
		int numP = 0;
		
		public void map(LongWritable key, Text value,Context context) 
				throws IOException, InterruptedException{
			
			Configuration conf = context.getConfiguration();
			int X1 = conf.getInt("X1",0);
			int Y1 = conf.getInt("Y1",0);
			int X2 = conf.getInt("X2",0);
			int Y2 = conf.getInt("Y2",0);
			
			Text outputKey = new Text();
			Text outputValue = new Text();
			String flag = "";
				
			// generate join key
			numP++;
			flag = "0";
			String point = value.toString();
			String[] parts = point.split(",");
			int X = Integer.parseInt(parts[0]);
			int Y = Integer.parseInt(parts[1]);
			outputValue.set(flag + "," + "p" + Integer.toString(numP) + "," + point);

			if (X1 == 0 && Y1 == 10001 && X2 == 10001 && Y2 == 0) {
				outputKey.set(windowLable(X, Y));
				context.write(outputKey, outputValue);
			} else {
				ArrayList<String> wListP = new ArrayList<String>();
				wListP = customerWindow(X1, Y1, X2, Y2);
				if (X >= Math.min(X1, X2) && X <= Math.max(X1, X2)
						&& Y >= Math.min(Y1, Y2) && Y <= Math.max(Y1, Y2)) {
					if (wListP.contains(windowLable(X, Y))) {
						// generate value
						outputKey.set(windowLable(X, Y));
						// write out
						context.write(outputKey, outputValue);
					}
				}
			}
			
		}
	}

	public static class MapR extends Mapper<LongWritable, Text, Text, Text>{
		int numR = 0;

		public void map(LongWritable key, Text value,Context context) 
				throws IOException, InterruptedException{

			Configuration conf = context.getConfiguration();
			int X1 = conf.getInt("X1",0);
			int Y1 = conf.getInt("Y1",0);
			int X2 = conf.getInt("X2",0);
			int Y2 = conf.getInt("Y2",0);
			
			Text outputKey = new Text();
			Text outputValue = new Text();
			String flag = "";
				
			// generate join key
			numR++;
			String rec = value.toString();
			String[] parts = rec.split(",");
			int topLeftX = Integer.parseInt(parts[0]);
			int topLeftY = Integer.parseInt(parts[1]);
			int bottomRightX = topLeftX + Integer.parseInt(parts[2]);
			int bottomRightY = topLeftY - Integer.parseInt(parts[3]);

			// generate join value
			flag = "1";
			outputValue.set(flag + "," + "r" + Integer.toString(numR) + ","
					+ rec);

			// write out
			String topLeft = windowLable(topLeftX,topLeftY);
			String bottomRight = windowLable(bottomRightX,bottomRightY);
			
			int wXmin = Integer.parseInt(topLeft.split(",")[0]);
			int wXmax = Integer.parseInt(bottomRight.split(",")[0]);
			
			int wYmin = Integer.parseInt(bottomRight.split(",")[1]);
			int wYmax = Integer.parseInt(topLeft.split(",")[1]);
			
			for (int i = wXmin; i <= wXmax; i++) {
				for (int j = wYmin; j <= wYmax; j++) {
					if (X1 == 0 && Y1 == 10001 && X2 == 10001 && Y2 == 0) {
						outputKey.set(Integer.toString(i) + "," + Integer.toString(j));
						context.write(outputKey, outputValue);
					} else {
						ArrayList<String> wListR = new ArrayList<String>();
						wListR = customerWindow(X1, Y1, X2, Y2);
						if (wListR.contains(Integer.toString(i) + "," + Integer.toString(j))) {
							outputKey.set(Integer.toString(i) + "," + Integer.toString(j));
							context.write(outputKey, outputValue);
						}
					}
					
				}
			}

		}
	}
	
	public static class Reduce extends Reducer<Text, Text, Text, Text>{	

		private HashMap<String,String> pointT = new HashMap<String,String>(); 
		private HashMap<String,String> recT = new HashMap<String,String>();

		String line = "";
		String flag = "";
		Text outkey = new Text();
		Text outvalue = new Text();
		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException{
			
			Iterator<Text> value = values.iterator();
			
			while (value.hasNext()) {
				line = value.next().toString().trim();
				String[] lineSplit = line.split(",");
				flag = lineSplit[0];

				if (flag.contains("0")) {
					String p = lineSplit[1];
					String pValue = lineSplit[2] + "," + lineSplit[3];
					pointT.put(p, pValue);
				} else {
					String r = lineSplit[1];
					String rValue = lineSplit[2] + "," + lineSplit[3] + ","
							+ lineSplit[4] + "," + lineSplit[5];
					recT.put(r, rValue);
				}
			}
			
			
			for ( String keyP : pointT.keySet()) {
				String point = pointT.get(keyP);
				String[] partP = point.split(",");
				int Px = Integer.parseInt(partP[0]);
				int Py = Integer.parseInt(partP[1]);
				
				for ( String keyR : recT.keySet()) {
					String rec = recT.get(keyR);
					String[] partR = rec.split(",");
					int Rx = Integer.parseInt(partR[0]);
					int Ry = Integer.parseInt(partR[1]);
					int Rw = Integer.parseInt(partR[2]);
					int Rl = Integer.parseInt(partR[3]);
					
					if ( Px>=Rx && Px<=(Rx+Rw) && Py>=(Ry-Rl) && Py<=Ry ) {
						outkey.set(keyR);
						outvalue.set("("+Integer.toString(Px)+","+Integer.toString(Py)+")");
						context.write(outkey, outvalue);
					}					
					
				}
			}
			
			pointT.clear();
			recT.clear();
			
		}
	}
	public class SimpleGroupingComparator extends WritableComparator {

		@Override
		public int compare(Text compositeKey1, Text compositeKey2) {
			return compare(compositeKey1.getNaturalKey(),compositeKey2.getNaturalKey());
		}
	}
	
	public class SimplePartitioner implements Partitioner {
		@Override
		public int getPartition(Text compositeKey, LongWritable value, int numReduceTasks) {
		//Split the key into natural and augment
			String naturalKey = compositeKey.toString().split("separator")
			return naturalKey.hashCode();
		}
	}
	
	
	public int run(String[] args) throws Exception {
		Job job = new Job();
		Configuration conf = job.getConfiguration();  
		
		int X1;
		int Y1;
		int X2;
		int Y2;
		String omit = "";
		Scanner dd = new Scanner(System.in);
		
		System.out.println("Do you need to define a window? (y/n) : ");
		omit = dd.next();
		
		if (omit.contains("n")) {
			conf.setInt("X1", 0);
			conf.setInt("Y1", 10001);
			conf.setInt("X2", 10001);
			conf.setInt("Y2", 0);
		} else {
			do {
				System.out.println("Enter X1: ");
				X1 = dd.nextInt();
			} while (X1 < 1 || X1 > 10000);
			conf.setInt("X1", X1);
			do {
				System.out.println("Enter Y1: ");
				Y1 = dd.nextInt();
			} while (Y1 < 1 || Y1 > 10000);
			conf.setInt("Y1", Y1);
			do {
				System.out.println("Enter X2: ");
				X2 = dd.nextInt();
			} while (X2 < 1 || X2 > 10000 || (X1 == X2));
			conf.setInt("X2", X2);
			do {
				System.out.println("Enter Y2: ");
				Y2 = dd.nextInt();
			} while (Y2 < 1 || Y2 > 10000 || (Y1 == Y2));
			conf.setInt("Y2", Y2);
		}
	   
	    job.setJobName("Q1");
	    job.setJarByClass(Q1.class);
		     
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	     
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	
//	    job.setMapperClass(Map.class);
	    job.setReducerClass(Reduce.class);
	    job.setNumReduceTasks(20);
	     
        job.setInputFormatClass(TextInputFormat.class);   
        job.setOutputFormatClass(TextOutputFormat.class);
	
	    MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MapP.class);
	    MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MapR.class);
	    FileOutputFormat.setOutputPath(job, new Path(args[2]));
	    job.waitForCompletion(true);
	    return job.isSuccessful()?0:1;
		
	}

	public static void main(String[] args) throws Exception {
				
//		ArrayList<String> wl = customerWindow(X1,Y1,X2,Y2);
//		for(String s: wl) {
//			System.out.println(s);
//		}

		int returnCode =  ToolRunner.run(new Q1(),args);
		System.exit(returnCode);
	}
	
}