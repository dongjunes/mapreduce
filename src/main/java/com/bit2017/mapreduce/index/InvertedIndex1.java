package com.bit2017.mapreduce.index;

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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class InvertedIndex1 {

	public static class MyMapper extends Mapper<Text, Text, Text, Text> {

		private static LongWritable one = new LongWritable(1L);
		private Text words = new Text();

		@Override
		protected void map(Text docId, Text contents, Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			String line = contents.toString();
			StringTokenizer token = new StringTokenizer(line, "\r\n\t,|()<> ''.:");
			while (token.hasMoreTokens()) {
				String tokens = token.nextToken().toLowerCase();
				words.set(tokens);
				context.write(words, docId);
			}

		}

	}

	public static class MyReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text word, Iterable<Text> docIds, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			StringBuilder s = new StringBuilder();
			boolean isFirst = true;
			for (Text docId : docIds) {
				if (isFirst != true) {
					s.append(",");
				} else {
					isFirst = false;
				}
				s.append(docId.toString());
			}
			context.write(word, new Text(s.toString()));
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "WordCount");

		// job init
		job.setJarByClass(InvertedIndex1.class);

		// mapper 지정
		job.setMapperClass(MyMapper.class);

		// reducer 지정
		job.setReducerClass(MyReducer.class);

		// map출력 키 타입
		job.setMapOutputKeyClass(Text.class);

		// map출력 타입지정
		job.setMapOutputValueClass(Text.class);

		// reduce출력 키 타입
		job.setOutputKeyClass(Text.class);

		// reduce출력 타입지정
		job.setOutputValueClass(Text.class);

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
