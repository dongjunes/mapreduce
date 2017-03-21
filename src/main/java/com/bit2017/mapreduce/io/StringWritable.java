package com.bit2017.mapreduce.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class StringWritable implements WritableComparable<StringWritable> {

	private String value;

	public void set(String value) {
		this.value = value;
	}
	public String get(){
		return value;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		value = WritableUtils.readString(in);

	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, value);
		
	}

	@Override
	public int compareTo(StringWritable o) {
		return value.compareTo(o.get());
	}

	@Override
	public String toString() {
		return value;
	}

}
