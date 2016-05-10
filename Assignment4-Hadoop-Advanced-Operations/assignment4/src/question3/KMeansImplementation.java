package question3;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

@SuppressWarnings("deprecation")
public class KMeansImplementation extends Configured implements Tool {
	HashMap<String, String> centroids = new HashMap<String, String>();
	public static class KMeansMap extends Mapper<Object, Text, Text, Text> {

		private HashMap<String, String> kcentroids = new HashMap<String, String>();

		protected void setup(Context context) throws IOException {
			Path[] cachePaths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for (Path p : cachePaths) {
				String filename = p.toString();
				if (filename.contains("q3_kcentroids")  || filename.contains("part-r-")) {
					BufferedReader br = new BufferedReader(new FileReader(p.toString()));
					String line = br.readLine();
					while (line != null) {
						String[] str = line.trim().split(",");
						kcentroids.put(str[0], str[1] + "," + str[2]);
						line = br.readLine();
					}
				}
			}
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();
			String[] lineSplit = line.split(",");
			int x = Integer.parseInt(lineSplit[0]);
			int y = Integer.parseInt(lineSplit[1]);
			String mapOutputKey = computeClustering(x, y);
			String mapOutputValue = value.toString() + ",1";
			context.write(new Text(mapOutputKey), new Text(mapOutputValue));
		}

		private String computeClustering(int x, int y) {
			int distance = Integer.MAX_VALUE;
			String centroid = "";

			for (Map.Entry<String, String> entry : kcentroids.entrySet()) {
				String key = entry.getKey();
				String[] values = entry.getValue().split(",");
				int xcentroid = Integer.parseInt(values[0]);
				int ycentroid = Integer.parseInt(values[1]);
				int temp = (xcentroid - x) * (xcentroid - x) + (ycentroid - y) * (ycentroid - y);
				if (temp < distance) {
					distance = temp;
					centroid = key;
				}
			}
			return centroid;
		}

	}
	
	public static class KMeansCombiner extends Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterable<Text> values, Context context) {
			Iterator<Text> value = values.iterator();
			long xSum = 0l;
			long ySum = 0l;
			long count = 0l;
			while (value.hasNext()) {
				String line = value.next().toString();
				String[] pointsInfo = line.trim().split(",");
				
				long x = Long.parseLong(pointsInfo[0]);
				long y = Long.parseLong(pointsInfo[1]);
				long num = Long.parseLong(pointsInfo[2]);
				xSum += x;
				ySum += y;
				count += num;
			}
			String outputValue = xSum + "," + ySum + "," + count;
			try {
				context.write(key, new Text(outputValue));
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public static class KMeansReduce extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			Iterator<Text> value = values.iterator();
			long xSum = 0l;
			long ySum = 0l;
			long count = 0l;
			while (value.hasNext()) {
				String line = value.next().toString();
				String[] pointsInfo = line.trim().split(",");

				long x = Long.parseLong(pointsInfo[0]);
				long y = Long.parseLong(pointsInfo[1]);
				long num = Long.parseLong(pointsInfo[2]);
				xSum += x;
				ySum += y;
				count += num;
			}

			long newX = xSum / count;
			long newY = ySum / count;
			String reduceOutputValue = newX + "," + newY;
			context.write(null, new Text(key.toString() + "," + reduceOutputValue));
		}
	}
	
	private boolean centroidsHaveChanged(Path currentPath) throws IOException{
		if (centroids.isEmpty()) return true;
		HashMap<String, String> newCentroids = new HashMap<String, String>();
		
		BufferedReader br = new BufferedReader(new FileReader(currentPath.toString()));
		String line = br.readLine();
		//get current centroids
		while (line != null) {
			String[] str = line.trim().split(",");
			newCentroids.put(str[0], str[1] + "," + str[2]);
			line = br.readLine();
		}
		
		//compare current with previous
		
		if(!centroids.values().equals(newCentroids.values())){
			centroids = newCentroids;
			return true;
		}
		
		return false;
	}
	
	public int run(String[] args) throws Exception {


		FileSystem fs;
		Path inputPath = new Path(args[0]);
		Path outputPath = null;
		
		Job job = null;
		for (int i = 0; i < 6; i++) {
			job = new Job();
			Configuration conf = job.getConfiguration();
			DistributedCache.addCacheFile(inputPath.toUri(), conf);
			fs = FileSystem.get(conf);
			
			if(!centroidsHaveChanged(inputPath)) break;

			outputPath = new Path(args[2] + i);
			
			job.setJobName("KMeans");
			job.setJarByClass(KMeansImplementation.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			job.setMapperClass(KMeansMap.class);
			job.setCombinerClass(KMeansCombiner.class);
			job.setReducerClass(KMeansReduce.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			FileInputFormat.addInputPath(job, new Path(args[1]));
			FileOutputFormat.setOutputPath(job, outputPath);
			job.waitForCompletion(true);
			
			inputPath = null;
			for(FileStatus file: fs.listStatus(outputPath)){
				if(file.getPath().toString().contains("part-r-")){
					inputPath = file.getPath();
					break;
				}
			};
			
			if(inputPath == null) break;
			
		}

		return job.isSuccessful() ? 0 : 1;

	}

	public static void main(String[] args) throws Exception {
		int returnCode = ToolRunner.run(new KMeansImplementation(), args);

	}

}