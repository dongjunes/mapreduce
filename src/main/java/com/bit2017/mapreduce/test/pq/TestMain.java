package com.bit2017.mapreduce.test.pq;

import java.util.PriorityQueue;

public class TestMain {

	public TestMain() {
		PriorityQueue<String> pq = new PriorityQueue<String>(10, new StringComparator());
		pq.add("hello");
		pq.add("hellos");
		pq.add("a");
		pq.add("qwer");
		pq.add("hellofvs");
		pq.add("cxvdcbscsa");
		while (!pq.isEmpty()) {
			String s = pq.remove();
			System.out.println(s);
		}
	}

	public static void main(String[] args) {
		new TestMain();
	}

}
