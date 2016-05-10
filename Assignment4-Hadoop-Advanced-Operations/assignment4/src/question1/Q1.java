package question1;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q1 {

	public static class PointsMapper extends Mapper<Object, Text, Text, Text> {

		private String[] windowParams = null; 
		
//		@Override
//		public void setup(Context context) throws IOException, InterruptedException {
//			Configuration conf = context.getConfiguration();
//			URI[] uris = DistributedCache.getCacheFiles(conf);
//			
//	        if(uris!=null){
//		        FSDataInputStream window = FileSystem.get(context.getConfiguration()).open(new Path(uris[0].toString().substring(0,uris[0].toString().length() - 4) + "_input.txt"));
//		        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(window));
//		        
//		        String windowLine = bufferedReader.readLine();
//		        windowParams = windowLine.split(",");
//	        }
//	    }
//		
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();
			String[] array = line.split(",");

			int x = Integer.parseInt(array[1]);
			int y = Integer.parseInt(array[2]);

			int rectanglekey = getVirtualRegion(x, y);

			Text mapkey = new Text("" + rectanglekey);
			String stringvalue = array[1] + "," + array[2];
			Text mapvalue = new Text(stringvalue);
			
			String window = context.getConfiguration().get("window");
//			context.write(null, new Text(window));
			
			if(window == null){
				context.write(mapkey, mapvalue);
			}else{
				windowParams = window.split(",");
				if (isPointInWindow(windowParams, stringvalue)){
					context.write(mapkey, mapvalue);
				}
			}
		}

		private boolean isPointInWindow(String[] window, String point){
			
			String[] pPoint = point.split(",");
			
			int x = Integer.parseInt(pPoint[0]);
			int y = Integer.parseInt(pPoint[1]);
			
			int xW = Integer.parseInt(window[0].substring(2));
			int yW = Integer.parseInt(window[1]);
			
			int xW2 = Integer.parseInt(window[2]);
			int yW2 = Integer.parseInt(window[3].substring(0, window[3].length()-1));
			
			if(x >= xW && x<= xW2 && y>= yW && y <= yW2){
				return true;
			}
			
			return false;
		}
		
		private int getVirtualRegion(int x, int y) {
			int row = y / 100;
			int column = x / 100 + 1;
			int rectanglekey = row * 100 + column;
			return rectanglekey;
		}
	}

	public static class RectangleMapper extends Mapper<Object, Text, Text, Text> {

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();
			String[] array = line.split(",");

			// get points of the rectangle

			int topCornerX = Integer.parseInt(array[1]);
			int topCornerY = Integer.parseInt(array[2]);
			int width = Integer.parseInt(array[3]);
			int height = Integer.parseInt(array[4]);

			ArrayList<Integer> keys = new ArrayList<Integer>();
			int currentRectKey = 0;

			for (int j = 1; j <= 4; j++) {
				switch (j) {
				case 1:
					currentRectKey = getVirtualRegion(topCornerX, topCornerY);
					break;
				case 2:
					currentRectKey = getVirtualRegion(topCornerX + width, topCornerY);
					break;
				case 3:
					currentRectKey = getVirtualRegion(topCornerX, topCornerY + height);
					break;
				case 4:
					currentRectKey = getVirtualRegion(topCornerX + width, topCornerY + height);
					break;
				}

				if (!keys.contains(currentRectKey)) {
					keys.add(currentRectKey);
					context.write(new Text(currentRectKey + ""), value);
				}
			}

		}

		private int getVirtualRegion(int x, int y) {
			int row = y / 100;
			int column = x / 100 + 1;
			int rectanglekey = row * 100 + column;
			return rectanglekey;
		}
	}


	public static class PointsReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			HashSet<String[]> rectset = new HashSet<String[]>();
			HashSet<String[]> pointset = new HashSet<String[]>();

			for (Text value : values) {
				String line = value.toString();
				String[] array = line.split(",");
				if (array.length == 2) {
					pointset.add(array);
				} else {
					rectset.add(array);
				}
				
//				context.write(null, new Text(value));

			}

			Iterator<String[]> rectIt = rectset.iterator();
			Iterator<String[]> pointIt = pointset.iterator();
			while (pointIt.hasNext()) {
				String[] point = pointIt.next();
				int x = Integer.parseInt(point[0]);
				int y = Integer.parseInt(point[1]);
				while (rectIt.hasNext()) {
					String[] rect = rectIt.next();
					int topleft_x = Integer.parseInt(rect[1]);
					int topleft_y = Integer.parseInt(rect[2]);
					int height = Integer.parseInt(rect[3]);
					int width = Integer.parseInt(rect[4]);
					if (x >= topleft_x && x <= topleft_x + width && y >= topleft_y && y <= topleft_y + height) {
						String output = "<" + rect[0] + "," + "(" + x + "," + y + ")>";
						context.write(null, new Text(output));
					}
				}
			}
		}
	}

	public static class PointRectangleComparator extends WritableComparator {

		protected PointRectangleComparator() {
			super(Text.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			String stringkey1 = ((Text) a).toString();
			String stringkey2 = ((Text) b).toString();

			if (stringkey1.charAt(0) == 'a') {
				stringkey1 = stringkey1.substring(1);
			}
			if (stringkey2.charAt(0) == 'a') {
				stringkey2 = stringkey2.substring(1);
			}

			int key1 = Integer.parseInt(stringkey1);
			int key2 = Integer.parseInt(stringkey2);

			return key1 - key2;

		}
	}

	public static class SortComparator extends WritableComparator {

		protected SortComparator() {
			super(Text.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			String stringkey1 = ((Text) a).toString();
			String stringkey2 = ((Text) b).toString();

			int length = stringkey1.length() - stringkey2.length();

			return length;

		}
	}

	public static class PointRectsPartitioner extends Partitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int numberOfReducers) {

			String partitionerkey = key.toString();
//			int keyInt = partitionerkey.charAt(0) == 'a' ? Integer.parseInt(partitionerkey.substring(1))
//					: Integer.parseInt(partitionerkey);

			if (numberOfReducers == 0) {
				return 0;
			}
			int keyInt = Integer.parseInt(partitionerkey);
			return keyInt % numberOfReducers;
		}

	}

	public static void main(String[] args) throws Exception {
	   Path outPath;
	   FileSystem dfs;
		
	   Configuration conf = new Configuration();
		
	   if(args.length > 3){
		   conf.set("window", args[2]);
	   }
		Job job = Job.getInstance(conf, "points rectangles information");
		job.setJarByClass(Q1.class);

		job.setMapperClass(RectangleMapper.class);
		job.setMapperClass(PointsMapper.class);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, RectangleMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PointsMapper.class);

		job.setNumReduceTasks(10);
		job.setPartitionerClass(PointRectsPartitioner.class);

		// job.setCombinerClass(PointsReducer.class);
		job.setReducerClass(PointsReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		   if(args.length > 3){
 			   //parameter passed
               outPath =  new Path(args[3]); 
 			   dfs = FileSystem.get(outPath.toUri(), conf);

     		   if(dfs.exists(outPath)){
     			   dfs.delete(outPath, true);
     		   }			   
 		   }else{
 			  outPath =  new Path(args[2]);
			   dfs = FileSystem.get(outPath.toUri(), conf);
 	 		   if(dfs.exists(outPath)){
 	 			   dfs.delete(outPath, true);
 	 		   }
 		   }
		   
		FileOutputFormat.setOutputPath(job, outPath);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
