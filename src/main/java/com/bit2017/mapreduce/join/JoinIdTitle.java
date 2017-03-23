package com.bit2017.mapreduce.join;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class JoinIdTitle {

	public static class TitleDocIdMapper extends Mapper<Text, Text, Text, Text> {

		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			context.write(value, new Text(key + "\t" + 1));
		}

	}

	public static class DocIdCiteCountMapper extends Mapper<Text, Text, Text, Text> {

		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			context.write(key, new Text(value + "\t" + 2));

		}

	}

	public static class JarIdTitleReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			int count = 0;
			Text k = new Text();
			Text v = new Text();

			for (Text value : values) {
				String info = value.toString();
				String[] tokens = info.split("\t");
				

				if ("1".equals(tokens[1])) {
					k.set(tokens[0] + "[" + key.toString() + "]");
				} else if ("2".equals(tokens[1])) {
					v.set(tokens[0]);
				} 

				count++;
			}

			// 출력
			if (count != 2) {
				return;
			}
			context.write(k, v);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "JoinIdTitle");

		job.setJarByClass(JoinIdTitle.class);
		final String TITLE_DOCID = args[0];
		final String DOCID_CITECOUNT = args[1];
		final String OUTPUT_DIR = args[2];

		// 입력
		MultipleInputs.addInputPath(job, new Path(TITLE_DOCID), KeyValueTextInputFormat.class, TitleDocIdMapper.class);
		MultipleInputs.addInputPath(job, new Path(DOCID_CITECOUNT), KeyValueTextInputFormat.class,
				DocIdCiteCountMapper.class);

		// 출력
		job.setReducerClass(JarIdTitleReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_DIR));

		// 실행
		job.waitForCompletion(true);
	}
}
