package com.bit2017.mapreduce.test.pq;

import java.util.Comparator;

public class StringComparator implements Comparator<String> {

	@Override
	public int compare(String o1, String o2) {
		if (o1.length() == o2.length())
			return 0;
		if (o1.length() > o2.length())
			return 1;
		return -1;
	}

}
