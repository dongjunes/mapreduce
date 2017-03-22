package com.bit2017.mapreduce.topn;

import java.util.Comparator;

public class ItemFreqComparator implements Comparator<ItemFreq> {

	@Override
	public int compare(ItemFreq o1, ItemFreq o2) {
		if (o1.getFreq() == o2.getFreq())
			return 0;
		if (o1.getFreq() > o2.getFreq())
			return 1;
		return -1;
	}

}
