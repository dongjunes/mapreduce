package com.bit2017.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import javax.sound.sampled.Line;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

import com.bit2017.mapreduce.WordCount.MyMapper;
import com.bit2017.mapreduce.WordCount.MyReducer;
import com.bit2017.mapreduce.io.NumberWritable;
import com.bit2017.mapreduce.io.StringWritable;

public class TextSearch {

	private static Log log = LogFactory.getLog(TextSearch.class);

	public static class MyMapper extends Mapper<LongWritable, Text, StringWritable, NumberWritable> {
		private String data;
		private static NumberWritable one = new NumberWritable(1L);
		private StringWritable words = new StringWritable();

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, StringWritable, NumberWritable>.Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			data = conf.get("data");

			log.info("map => data : " + data);

			String line = value.toString();

			StringTokenizer token = new StringTokenizer(line, "\r\n\t,|()<> ''.:");
			while (line.contains(data)) {
				
					words.set(token.nextToken().toLowerCase());
					context.write(words, one);
				
			}

		}

	}

	public static class MyReducer extends Reducer<StringWritable, NumberWritable, StringWritable, NumberWritable> {

		private NumberWritable sumWritable = new NumberWritable();

		@Override
		protected void reduce(StringWritable key, Iterable<NumberWritable> values,
				Reducer<StringWritable, NumberWritable, StringWritable, NumberWritable>.Context context)
				throws IOException, InterruptedException {
			long sum = 0;
			for (NumberWritable value : values) {
				sum += value.get();
			}
			sumWritable.set(sum);

			context.getCounter("Word Status", "Count of all Words").increment(sum);

			context.write(key, sumWritable);

		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("data", args[2]);

		Job job = new Job(conf, "TextSearch");
		// job init
		job.setJarByClass(TextSearch.class);

		// mapper 지정
		job.setMapperClass(MyMapper.class);

		// reducer 지정
		job.setReducerClass(MyReducer.class);

		// 출력 키 타입
		job.setMapOutputKeyClass(StringWritable.class);

		// 출력 타입지정
		job.setMapOutputValueClass(NumberWritable.class);

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
