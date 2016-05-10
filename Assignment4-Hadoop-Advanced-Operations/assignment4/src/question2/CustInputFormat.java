package question2;

import java.io.IOException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

/**
* Reads records that are delimited by a specific begin/end tag.
*/
public class CustInputFormat extends TextInputFormat {
	public static final String START_TAG_KEY = "jsoninput.start";
	public static final String END_TAG_KEY = "jsoninput.end";
	public static final String FIELD_SEPARATOR = ",\n";
	public static final String DATA_SEPARATOR = ":";
	public static final String LINE_SEPARATOR = "\n";
	
	@Override
	public RecordReader<LongWritable,Text> getRecordReader(InputSplit inputSplit,
			JobConf jobConf,
			Reporter reporter) throws IOException {
		return new JsonRecordReader((FileSplit) inputSplit, jobConf);
	}
	/**
	* XMLRecordReader class to read through a given xml document to output xml
	* blocks as records as specified by the start tag and end tag
	*
	*/
	public static class JsonRecordReader implements
		RecordReader<LongWritable,Text> {
		private final byte[] startTag;
		private final byte[] endTag;
		private final long start;
		private final long end;
		private final FSDataInputStream fsin;
		private final DataOutputBuffer buffer = new DataOutputBuffer();
//		private final DataOutputBuffer resultBuffer = new DataOutputBuffer();
		private final DataInputBuffer in = new DataInputBuffer();

		
		public JsonRecordReader(FileSplit split, JobConf jobConf) throws IOException {
			startTag = jobConf.get(START_TAG_KEY).getBytes("utf-8");
			endTag = jobConf.get(END_TAG_KEY).getBytes("utf-8");
			// open the file and seek to the start of the split
			start = split.getStart();
			end = start + split.getLength();
			Path file = split.getPath();
			FileSystem fs = file.getFileSystem(jobConf);
			fsin = fs.open(split.getPath());
			fsin.seek(start);
		}
		
		@Override
		public boolean next(LongWritable key, Text value) throws IOException {
			String results = "";
			 String fieldValue;
			 String tempResults="";
//			 try{
				 if (fsin.getPos() < end) {
					 if (readUntilMatch(startTag, false)) {
						 try {
							 buffer.write(startTag);
//							 buffer.reset();
							 if (readUntilMatch(endTag, true)) {
								 key.set(fsin.getPos());
								 //convert the data to a customer data string
								 tempResults = new String( buffer.getData());
								 
								 String[] fields = tempResults.split(FIELD_SEPARATOR);
								 //get the individual fields
								 for(String field : fields){
									 String[] fieldValuePair = field.split(DATA_SEPARATOR);
									 if(fieldValuePair.length == 2){
										 fieldValue = fieldValuePair[1] ;
										 if(fieldValue.contains("\"")){
											 fieldValue = fieldValue.substring(1, fieldValue.length()-1);
										 }
										 results += fieldValue + ",";
									 }
									 
								 }
								 if(results.length()>= 2){
									 results = results.substring(0, results.length()-1);				 
									 //convert to text and split by \n. 
									 value.set(results);
								 }else{
									 throw new IOException();
								 }
								 return true;
							 }
						 } finally {
							 buffer.reset();
						 }
					 }
				 }
//			 }catch(Exception e){
//				 throw e;
//				 //throw new IOException("The file is not properly formatted");
//			 }
			 return false;
		}
	
		@Override
		public LongWritable createKey() {
			return new LongWritable();
		}
	
		@Override
		public Text createValue() {
			return new Text();
		}
		@Override
		public long getPos() throws IOException {
			return fsin.getPos();
		}
		
		@Override
		public void close() throws IOException {
			fsin.close();
		}
		@Override
		public float getProgress() throws IOException {
			return (fsin.getPos() - start) / (float) (end - start);
		}
		
		private boolean readUntilMatch(byte[] match, boolean withinBlock) throws IOException {
			int i = 0;
			while (true) {
				int b = fsin.read();
				// end of file:
				if (b == -1) return false;
				// save to buffer:
				if (withinBlock) buffer.write(b);
				// check if we're matching:
				if (b == match[i]) {
					i++;
					if (i >= match.length) return true;
				} else i = 0;
				// see if we've passed the stop point:
				if (!withinBlock && i == 0 && fsin.getPos() >= end) return false;
			}
		}
	}
}