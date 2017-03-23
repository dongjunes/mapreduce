package com.bit2017.mapreduce.textsearch;

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
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class SearchDocs {
	public static class MyMapper extends Mapper<Text, Text, Text, LongWritable> {
		private Text words = new Text();
		private LongWritable one = null;

		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String findWord = conf.get("data");
			String line = value.toString();
			StringTokenizer token = new StringTokenizer(line, "\r\n\t,|()<>''.:");

			int lastIndex = 0;
			Integer count = 0;
			while (lastIndex != -1) {
				lastIndex = line.indexOf(findWord, lastIndex);
				if (lastIndex != -1) {
					count++;
					lastIndex += findWord.length();
				}
				one = new LongWritable(Long.valueOf(count.toString()));

			}
			words.set(findWord);
			context.write(words, one);
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
		conf.set("data", args[2]);

		Job job = new Job(conf, "SearchDocs");
		// job init
		job.setJarByClass(SearchDocs.class);

		// mapper 지정
		job.setMapperClass(MyMapper.class);

		// reducer 지정
		job.setReducerClass(MyReducer.class);

		// 출력 키 타입
		job.setMapOutputKeyClass(Text.class);

		// 출력 타입지정
		job.setMapOutputValueClass(LongWritable.class);

		// 입력 파일포멧 지정(생략가능
		job.setInputFormatClass(KeyValueTextInputFormat.class);

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
