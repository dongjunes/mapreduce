package com.bit2017.mapreduce.topn;

import java.io.IOException;
import java.util.PriorityQueue;

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

public class TopN {
	public static class MyMapper extends Mapper<Text, Text, Text, LongWritable> {
		private int topN = 10;
		private PriorityQueue<ItemFreq> pq = null;

		@Override
		protected void setup(Mapper<Text, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			topN = context.getConfiguration().getInt("topN", 10);
			pq = new PriorityQueue<ItemFreq>(10, new ItemFreqComparator());
		}

		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			ItemFreq newItemFreq = new ItemFreq();
			newItemFreq.setItem(key.toString());
			newItemFreq.setFreq(Long.parseLong(value.toString()));

			ItemFreq head = pq.peek();
			if (head == null || head.getFreq() < newItemFreq.getFreq()) {
				pq.add(newItemFreq);
			}

			if (pq.size() > topN) {
				pq.remove();
			}

		}

		@Override
		protected void cleanup(Mapper<Text, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			while (!pq.isEmpty()) {
				ItemFreq itemFreq = pq.remove();
				context.write(new Text(itemFreq.getItem()), new LongWritable(itemFreq.getFreq()));
			}
		}

	}

	public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		private int topN = 10;
		private PriorityQueue<ItemFreq> pq = null;

		@Override
		protected void setup(Reducer<Text, LongWritable, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			topN = context.getConfiguration().getInt("topN", 10);
			pq = new PriorityQueue<ItemFreq>(10, new ItemFreqComparator());
		}

		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,
				Reducer<Text, LongWritable, Text, LongWritable>.Context arg2) throws IOException, InterruptedException {

			Long sum = 0L;
			for (LongWritable value : values) {
				sum += value.get();
			}

			ItemFreq newItemFreq = new ItemFreq();
			newItemFreq.setItem(key.toString());
			newItemFreq.setFreq(sum);

			ItemFreq head = pq.peek();
			if (head == null || head.getFreq() < newItemFreq.getFreq()) {
				pq.add(newItemFreq);
			}

			if (pq.size() > topN) {
				pq.remove();
			}

		}

		@Override
		protected void cleanup(Reducer<Text, LongWritable, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			while (!pq.isEmpty()) {
				ItemFreq itemFreq = pq.remove();
				context.write(new Text(itemFreq.getItem()), new LongWritable(itemFreq.getFreq()));
			}
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "TopN");

		// job init
		job.setJarByClass(TopN.class);

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

		// N 파라미터
		job.getConfiguration().setInt("topN", Integer.parseInt(args[2]));

		// 실행
		job.waitForCompletion(true);

	}
}
