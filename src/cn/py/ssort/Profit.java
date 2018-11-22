package cn.py.ssort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Profit implements WritableComparable<Profit>{

	private int month;
	private String name;
	private int fee;
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(month);
		out.writeUTF(name);
		out.writeInt(fee);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.month = in.readInt();
		this.name = in.readUTF();
		this.fee = in.readInt();
	}

	@Override
	public int compareTo(Profit o) {
		int result = this.month-o.month;
		if(result == 0){
			return o.fee-this.fee;
		}else {
			return result;
		}
	}

	public int getMonth() {
		return month;
	}

	public void setMonth(int month) {
		this.month = month;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getFee() {
		return fee;
	}

	public void setFee(int fee) {
		this.fee = fee;
	}

	@Override
	public String toString() {
		return "Profit [month=" + month + ", name=" + name + ", fee=" + fee + "]";
	}
	
	
}
