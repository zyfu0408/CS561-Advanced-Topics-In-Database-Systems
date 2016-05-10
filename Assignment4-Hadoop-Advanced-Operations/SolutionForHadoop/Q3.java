package assn4.problem2;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
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
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Q3 extends Configured implements Tool {

    public static List<ArrayList<Double>> getCenters(String inputpath){  
        List<ArrayList<Double>> result = new ArrayList<ArrayList<Double>>();  
        Configuration conf = new Configuration();  
        try {  
            FileSystem hdfs = FileSystem.get(conf);  
            Path in = new Path(inputpath);  
            FSDataInputStream fsIn = hdfs.open(in);  
            LineReader lineIn = new LineReader(fsIn, conf);  
            Text line = new Text();  
            while (lineIn.readLine(line) > 0){  
                String record = line.toString();  
                String[] fields = record.split("\t");  
                String point = fields[1];
                String[] parts = point.split(",");
                List<Double> tmplist = new ArrayList<Double>();  
                for (int i = 0; i < parts.length; ++i){  
                    tmplist.add(Double.parseDouble(parts[i]));  
                }  
                result.add((ArrayList<Double>) tmplist);  
            }  
            fsIn.close();  
        } catch (IOException e){  
            e.printStackTrace();  
        }  
        return result;  
    } 
    
    public static double calDist(String oldseed, String newseed)  
    throws IOException{  
        List<ArrayList<Double>> oldcenters = getCenters(oldseed);  
        List<ArrayList<Double>> newcenters = getCenters(newseed);  
        double distance = 0;
        int k = oldcenters.size();
        for (int i = 0; i < k; i++){  
            for (int j = 0; j < oldcenters.get(i).size(); j++){  
                double tmp = Math.abs(oldcenters.get(i).get(j) - newcenters.get(i).get(j));  
                distance += Math.pow(tmp, 2);  
            }  
        }  
        System.out.println("Distance = " + distance );  
        return distance;  
    }
	
    public static int calSize(String inputpath) {
        List<ArrayList<Double>> oldcenters = getCenters(inputpath);  
        int k = oldcenters.size();
        return k;
    }
    
	public static int createRamdomInt(int min, int max){
        Random random = new Random();
        int s = random.nextInt(max - min + 1) + min;
        return s;
	}
	
	public static void seedFile(int k) {
		int X;
		int Y;
		StringBuffer str = new StringBuffer();
		
		FileWriter fw;
		try {
			fw = new FileWriter("/home/hadoop/Desktop/CS561_Project3/kMeans_seed.txt");
			BufferedWriter bw = new BufferedWriter(fw,1);
			
			for(int i = 0; i<k; i++){
				//set cluster number
				str.append(Integer.toString(i+1));
				str.append('\t');
				//set x
				X = createRamdomInt(1, 10000);
				str.append(Integer.toString(X));
				str.append(',');
				//set y
				Y = createRamdomInt(1, 10000);
				str.append(Integer.toString(Y));
				
				bw.write(str.toString());
				bw.newLine();
				str.setLength(0);
			}
//			bw.write("Tag"+"\t"+"Yes");
			bw.flush();
			bw.close();
			fw.close();
			System.out.println("Done!");

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
    
    
	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		
		private HashMap<String,String> seeds = new HashMap<String,String>();
		private Text outputKey = new Text();
		private Text outputValue = new Text();
		
		protected void setup(Context context) throws IOException {
			Path[] cachePaths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for(Path p:cachePaths){
				if(p.toString().endsWith("kMeans_seed.txt")){
					BufferedReader br = new BufferedReader(new FileReader(p.toString()));
					String line=null;
					while((line=br.readLine()) != null){
						String[] parts = line.split("\t");
						seeds.put(parts[0], parts[1]);
					}
				}
			}	
		}
		
		public void map(LongWritable key, Text value,Context context) 
				throws IOException, InterruptedException{
			
			String cl = "";
			double dist = Double.MAX_VALUE;
			
			String line = value.toString();
			String[] parts = line.split(",");
			int X = Integer.parseInt(parts[0]);
			int Y = Integer.parseInt(parts[1]);
			
			for(String k: seeds.keySet()) {
				String seed_str = seeds.get(k);
				String[] seed_parts = seed_str.split(",");
				double seed_X = Double.parseDouble(seed_parts[0]);
				double seed_Y = Double.parseDouble(seed_parts[1]);
				double pointToSeed = Math.sqrt(Math.pow((X-seed_X), 2)+Math.pow((Y-seed_Y), 2));
				if (pointToSeed<dist) {
					cl = k;
					dist = pointToSeed;
				}
			}
			
			outputKey.set(cl);
			outputValue.set(line+","+Integer.toString(1));
			context.write(outputKey, outputValue);
			
		}
	}
	
	
	public static class MapForCluster extends Mapper<LongWritable, Text, Text, Text>{
		
		private HashMap<String,String> seeds = new HashMap<String,String>();
		private Text outputKey = new Text();
		private Text outputValue = new Text();
		
		protected void setup(Context context) throws IOException {
			Path[] cachePaths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for(Path p:cachePaths){
				if(p.toString().endsWith("kMeans_seed.txt")){
					BufferedReader br = new BufferedReader(new FileReader(p.toString()));
					String line=null;
					while((line=br.readLine()) != null){
						String[] parts = line.split("\t");
						seeds.put(parts[0], parts[1]);
					}
				}
			}	
		}
		
		public void map(LongWritable key, Text value,Context context) 
				throws IOException, InterruptedException{
			
			String cl = "";
			double dist = Double.MAX_VALUE;
			
			String line = value.toString();
			String[] parts = line.split(",");
			int X = Integer.parseInt(parts[0]);
			int Y = Integer.parseInt(parts[1]);
			
			for(String k: seeds.keySet()) {
				String seed_str = seeds.get(k);
				String[] seed_parts = seed_str.split(",");
				double seed_X = Double.parseDouble(seed_parts[0]);
				double seed_Y = Double.parseDouble(seed_parts[1]);
				double pointToSeed = Math.sqrt(Math.pow((X-seed_X), 2)+Math.pow((Y-seed_Y), 2));
				if (pointToSeed<dist) {
					cl = k;
					dist = pointToSeed;
				}
			}
			
			outputKey.set(cl);
			outputValue.set(line);
			context.write(outputKey, outputValue);
			
		}
	}


	

	public static class Combine extends Reducer<Text, Text, Text, Text>{	

		private String line = "";
		private Text outkey = new Text();
		private Text outvalue = new Text();
		
		public void combine(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException{
			
			long sumX = 0;
			long sumY = 0;
			long count = 0;
			int X = 0;
			int Y = 0;
			Iterator<Text> value = values.iterator();
			
			while (value.hasNext()) {
				String line = value.next().toString().trim();
				String[] parts = line.split(",");
				X = Integer.parseInt(parts[0]);
				Y = Integer.parseInt(parts[1]);
				sumX += X;
				sumY += Y;
				count++;
			}
			
			outkey.set(key);
			outvalue.set(Long.toString(sumX)+","+Long.toString(sumY)+","+Long.toString(count));
			context.write(outkey, outvalue);
			
		}
	}

	
	public static class Reduce extends Reducer<Text, Text, Text, Text>{	

		private String line = "";
		private Text outkey = new Text();
		private Text outvalue = new Text();
		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException{
			
			long sumXFinal = 0;
			long sumYFinal = 0;
			long countFinal = 0;
			double meanX = 0;
			double meanY = 0;
			long sumX = 0;
			long sumY = 0;
			long count = 0;
			Iterator<Text> value = values.iterator();
			
			while (value.hasNext()) {
				String line = value.next().toString().trim();
				String[] parts = line.split(",");
				sumX = Long.parseLong(parts[0]);
				sumY = Long.parseLong(parts[1]);
				count = Long.parseLong(parts[2]);
				sumXFinal += sumX;
				sumYFinal += sumY;
				countFinal += count;
			}
			
			meanX = (double)sumXFinal/(double)countFinal;
			meanY = (double)sumYFinal/(double)countFinal;

			outkey.set(key);
			outvalue.set(Double.toString(meanX)+","+Double.toString(meanY));
			context.write(outkey, outvalue);
			
		}
	}
	
	
	public int run(String[] args) throws Exception {
		
		Job job = new Job();
		Configuration conf = job.getConfiguration();
		DistributedCache.addCacheFile(new Path("/user/hadoop/Project3/kMeans_seed.txt").toUri(), conf);
		conf.set("mapreduce.output.textoutputformat.separator", ",");
		
	    job.setJobName("Q3");
	    job.setJarByClass(Q3.class);
		     
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	     
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	
	    job.setMapperClass(Map.class);
	    job.setCombinerClass(Combine.class);
	    job.setReducerClass(Reduce.class);
//	    job.setNumReduceTasks(20);
	     
        job.setInputFormatClass(TextInputFormat.class);   
        job.setOutputFormatClass(TextOutputFormat.class);
	
	    FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
               
	    job.waitForCompletion(true);
	    return job.isSuccessful()?0:1;
	    
	}

	public static void main(String[] args) throws Exception {
		
		int repeated = 0;
		double dist = Double.MAX_VALUE;
		boolean tag = true;
		
		int k = Integer.parseInt(args[2]);
		double threshold = Double.parseDouble(args[3]);
		seedFile(k); // Generate local file kMeans_seed.txt
		Configuration cf = new Configuration();
		FileSystem fs = FileSystem.get(cf);
		fs.moveFromLocalFile(new Path("/home/hadoop/Desktop/CS561_Project3/kMeans_seed.txt"), 
								new Path("/user/hadoop/Project3/kMeans_seed.txt"));  // put local file to HDFS
		
//		int returnCode = ToolRunner.run(new Q3(), args);
		
		do {
			int returnCode = ToolRunner.run(new Q3(), args);

			Configuration conf = new Configuration();
			FileSystem hdfs = FileSystem.get(conf);
			
			Path oldseed = new Path("/user/hadoop/Project3/kMeans_seed.txt");
			Path newseed = new Path("/user/hadoop/Project3/output_problem3/part-r-00000");
			int sizeOld = calSize(oldseed.toString());
			int sizeNew = calSize(newseed.toString());
			if (sizeOld==sizeNew) {
				dist = calDist(oldseed.toString(), newseed.toString());
			}else{
				tag = false;
			}

			hdfs.copyToLocalFile(new Path("/user/hadoop/Project3/output_problem3/part-r-00000"), 
									new Path("/home/hadoop/Desktop/CS561_Project3/Q3out.txt"));
			hdfs.delete(new Path("/user/hadoop/Project3/kMeans_seed.txt"),true);
			hdfs.moveFromLocalFile(new Path("/home/hadoop/Desktop/CS561_Project3/Q3out.txt"),
									new Path("/user/hadoop/Project3/kMeans_seed.txt"));
			hdfs.delete(new Path(args[1]), true);
			
			// Delete local file "~/Q3out/part-r-00000"
			File index = new File("/home/hadoop/Desktop/CS561_Project3/Q3out.txt");
			index.delete();

			++repeated;
			System.out.println("Repeated:" + repeated);
			
		} while (repeated<6 && dist>threshold && tag==true); // dist > Threshold
		

		// Clusetering based on the newest seeds
		Job job = new Job();
		Configuration conf = job.getConfiguration();
		DistributedCache.addCacheFile(new Path("/user/hadoop/Project3/kMeans_seed.txt").toUri(), conf);
		
	    job.setJobName("Q3cluster");
	    job.setJarByClass(Q3.class);
		     
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	     
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	
	    job.setMapperClass(MapForCluster.class);
	    job.setNumReduceTasks(5);
	     
        job.setInputFormatClass(TextInputFormat.class);   
        job.setOutputFormatClass(TextOutputFormat.class);
	
	    FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
               
	    job.waitForCompletion(true);
		
		
		FileWriter fw;
		try {
			fw = new FileWriter("/home/hadoop/Desktop/CS561_Project3/Change.txt");
			BufferedWriter bw = new BufferedWriter(fw,1);
			StringBuffer str = new StringBuffer();

			if (dist > threshold) {
				str.append("Changed" + "\t" + "Yes");
			} else {
				str.append("Changed" + "\t" + "No");
			}
			bw.write(str.toString());
			bw.flush();
			bw.close();
			fw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		fs.moveFromLocalFile(new Path("/home/hadoop/Desktop/CS561_Project3/Change.txt"), 
								new Path("/user/hadoop/Project3/output_problem3"));
		
	}
	
}


