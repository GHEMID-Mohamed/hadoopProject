
public class StringAndInt implements Comparable<StringAndInt> {

	private String tag;
	private int occurences;
	
	public StringAndInt(String tag, int occurences) {
		// TODO Auto-generated constructor stub
		super();
		this.tag = tag;
		this.occurences = occurences;

	}
	
	public String getTag() {
		return tag;
	}
	
	public int getOccurence() {
		return occurences;
	}


	@Override
	public int compareTo(StringAndInt stringAndInt) {
		// TODO Auto-generated method stub
		return stringAndInt.occurences - this.occurences;
	}

}
