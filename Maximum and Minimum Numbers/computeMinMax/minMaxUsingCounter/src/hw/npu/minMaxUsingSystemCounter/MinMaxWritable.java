package hw.npu.minMaxUsingSystemCounter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class MinMaxWritable implements Writable{

	private int min;
	private int max;
	private int countInvalid;
	
	@Override
	public void readFields(DataInput in) throws IOException {
		min	= in.readInt();
		max = in.readInt();
		countInvalid = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {

		out.writeInt(min);
		out.writeInt(max);
		out.writeInt(countInvalid);
	}

	public int getMin() {
		return min;
	}

	public void setMin(int min) {
		this.min = min;
	}

	public int getMax() {
		return max;
	}

	public void setMax(int max) {
		this.max = max;
	}

	public int getCountInvalid() {
		return countInvalid;
	}

	public void setCountInvalid(int countInvalid) {
		this.countInvalid = countInvalid;
	}
	
	@Override
	public String toString() {
		return "Minimum: "+ min + "\t"+ "Maximum: "+ max+ "\t"+ "malformed: " + countInvalid  ;
	}

}
