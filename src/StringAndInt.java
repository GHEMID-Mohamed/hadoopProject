import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class StringAndInt implements Comparable<StringAndInt>, Writable {

	private Text tag;
	private IntWritable occurences;

	// Constructor sans arguments
		public StringAndInt() {
			// TODO Auto-generated constructor stub
			this.tag = new Text();
			this.occurences = new IntWritable();
		}
	
	// Constructor for Text and IntWritable
	public StringAndInt(Text tag, IntWritable occurences) {
		// TODO Auto-generated constructor stub
		this.tag = tag;
		this.occurences = occurences;
	}

	// Constructor for String and Int
	public StringAndInt(String tag, int occurences) {
		// TODO Auto-generated constructor stub
		this.tag = new Text(tag);
		this.occurences = new IntWritable(occurences);
	}

	public String getTag() {
		return tag.toString();
	}

	public int getOccurence() {
		return occurences.get();
	}

	@Override
	public int compareTo(StringAndInt stringAndInt) {
		// TODO Auto-generated method stub
		return stringAndInt.occurences.get() - this.occurences.get();
	}

	public void readFields(DataInput in) throws IOException {
		tag.readFields(in);
		occurences.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		tag.write(out);
		occurences.write(out);
	}

}
