package com.bit2017.mapreduce.test.string;

import java.util.StringTokenizer;

public class Trigram {

	public static void main(String[] args) {
		String s = "the Art of Map Reduce Programming";
		trigram(s);
	}

	public static void trigram(String s) {
		StringTokenizer token = new StringTokenizer(s, "\r\n\t,|()<>''.: ");
		String firstWord = token.nextToken();
		String secondWord = token.nextToken();
		while (token.hasMoreTokens()) {
			String thirdWord = token.nextToken();

			String trigram = firstWord + " " + secondWord + " " + thirdWord;
			firstWord = secondWord;
			secondWord = thirdWord;
			System.out.println(trigram);
		}

	}

}
