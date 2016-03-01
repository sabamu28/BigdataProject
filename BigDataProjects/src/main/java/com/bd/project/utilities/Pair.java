package com.bd.project.utilities;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Pair implements WritableComparable<Pair> {
	String firstValue;
	String secondValue;

	public Pair() {
	}

	public Pair(String item, String item2) {
		this.firstValue = item;
		this.secondValue = item2;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((firstValue == null) ? 0 : firstValue.hashCode());
		result = prime * result + ((secondValue == null) ? 0 : secondValue.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Pair other = (Pair) obj;
		if (firstValue == null) {
			if (other.firstValue != null)
				return false;
		} else if (!firstValue.equals(other.firstValue))
			return false;
		if (secondValue == null) {
			if (other.secondValue != null)
				return false;
		} else if (!secondValue.equals(other.secondValue))
			return false;
		return true;
	}

	public String getFirstPair() {
		return firstValue;
	}
	
	public void setFirstPair(String a) {
		this.firstValue = a;
	}

	public String getSecondPair() {
		return secondValue;
	}

	public void setSecondPair(String b) {
		this.secondValue = b;
	}

	@Override
	public String toString() {
		return "Pair [" + firstValue + ", " + secondValue + "]";
	}
	
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(firstValue.toString());
		out.writeUTF(secondValue.toString());

	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		firstValue = in.readUTF();
		secondValue = in.readUTF();

	}

	public int compareTo(Pair o) {
		int cmp = firstValue.compareTo(o.firstValue);
		if (cmp != 0)
			return cmp;
		if (this.secondValue.toString() == "*")
			return -1;
		else if (o.secondValue.toString() == "*")
			return 1;
		return secondValue.compareTo(o.secondValue);
	}

}
