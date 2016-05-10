package util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class TaggedKey implements Writable, WritableComparable<TaggedKey> {

    private Text joinKey = new Text();
    private IntWritable tag = new IntWritable();

    @Override
    public int compareTo(TaggedKey taggedKey) {
        int compareValue = this.joinKey.compareTo(taggedKey.getJoinKey());
        if(compareValue == 0 ){
            compareValue = this.tag.compareTo(taggedKey.getTag());
        }
       return compareValue;
    }
   //Details left out for clarity

	Text  getJoinKey() {
		return joinKey;
	}

	private IntWritable getTag() {
		return tag;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		
	}
 }
