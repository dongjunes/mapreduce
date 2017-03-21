package com.bit2017.mapreduce.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class NumberWritable implements Writable {

	private Long number;

	public NumberWritable() {
	}
	
	public NumberWritable( Long number ) {
		this.number = number;
	}

	public void set( Long number) {
		this.number = number;
	}
	
	public Long get() {
		return number;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		number = WritableUtils.readVLong( in );
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeVLong( out, number );
	}

	@Override
	public String toString() {
		return String.valueOf( number );
	}
}
