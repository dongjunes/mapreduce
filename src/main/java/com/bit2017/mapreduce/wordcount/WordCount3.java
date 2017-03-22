package com.bit2017.mapreduce.wordcount;

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

public class WordCount3 {

	public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
		private Text word = new Text();
		private static LongWritable one = new LongWritable(1L);

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line, "\r\n\t,|()<> ''.:");
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken().toLowerCase());
				context.write(word, one);
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

			context.getCounter("Word Status", "Count of all Words").increment(sum);
			context.getCounter("Word Status", "Count of unique Word").increment(1);

			context.write(key, sumWritable);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "WordCount3");

		// Job Instance 초기화 작업
		job.setJarByClass(WordCount3.class);

		// 맵퍼 클래스 지정
		job.setMapperClass(MyMapper.class);

		// 리듀서 클래스 지정
		job.setReducerClass(MyReducer.class);

		// 리듀스 개 수 지정
		job.setNumReduceTasks(2);

		// 컴바이너 세팅
		job.setCombinerClass(MyReducer.class);

		// 출력 키 타입
		job.setMapOutputKeyClass(Text.class);

		// 출력 밸류 타입
		job.setMapOutputValueClass(LongWritable.class);

		// 입력파일 포맷 지정
		job.setInputFormatClass(TextInputFormat.class);
		// 출력파일 포맷 지정(생략 가능)
		job.setOutputFormatClass(TextOutputFormat.class);

		// 입력 파일 이름 지정
		FileInputFormat.addInputPath(job, new Path(args[0]));

		// 출력 디렉토리 지정
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 실행
		job.waitForCompletion(true);
	}
}
