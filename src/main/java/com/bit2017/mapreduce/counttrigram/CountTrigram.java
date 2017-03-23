package com.bit2017.mapreduce.counttrigram;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.bit2017.mapreduce.wordcount.WordCount;

public class CountTrigram {
	public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

		private static LongWritable one = new LongWritable(1L);
		private Text words = new Text();

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {

			String line = value.toString();
			StringTokenizer token = new StringTokenizer(line, "\r\n\t,|()<> ''.:");

			line = "hi a b c f g k";
			String firstWord = token.nextToken();
			String secondWord = token.nextToken();

			if (token.hasMoreTokens()) {
				while (token.hasMoreTokens()) {
					String thirdWord = token.nextToken();
					String trigram = firstWord + " " + secondWord + " " + thirdWord;

					words.set(trigram);
					context.write(words, one);

					firstWord = secondWord;
					secondWord = thirdWord;
				}
			} else {
				System.out.println("문장의 단어가 2개 이하입니다.");
				return;
			}

		}

	}

	public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

		private LongWritable sumWritable = new LongWritable();

		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,
				Reducer<Text, LongWritable, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			long sum = 0;
			for (LongWritable value : values) {
				sum += value.get();
			}
			sumWritable.set(sum);
			context.write(key, sumWritable);

		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "WordCount");

		// job init
		job.setJarByClass(WordCount.class);

		// mapper 지정
		job.setMapperClass(MyMapper.class);

		// reducer 지정
		job.setReducerClass(MyReducer.class);

		// 출력 키 타입
		job.setMapOutputKeyClass(Text.class);

		// 출력 타입지정
		job.setMapOutputValueClass(LongWritable.class);

		// 입력 파일포멧 지정(생략가능
		job.setInputFormatClass(TextInputFormat.class);

		// 출력 파일포멧 지정(생략가능
		job.setOutputFormatClass(TextOutputFormat.class);

		// 입력파일 이름지정
		FileInputFormat.addInputPath(job, new Path(args[0]));

		// 출력디렉토리 지정
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 실행
		job.waitForCompletion(true);
	}

}
