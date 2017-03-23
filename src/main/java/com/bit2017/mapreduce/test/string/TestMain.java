package com.bit2017.mapreduce.test.string;

public class TestMain {

	public static void main(String[] args) {
		String str = "hellokhellosdajfkohello";
		String findStr = "hello";
		int lastIndex = 0;
		int count = 0;

		while (lastIndex != -1) {
			lastIndex = str.indexOf(findStr, lastIndex);
			if (lastIndex != -1) {
				count++;
				lastIndex += findStr.length();
			}
		}
		System.out.println(count);

	}

}
