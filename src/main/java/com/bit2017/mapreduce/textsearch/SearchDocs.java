package com.bit2017.mapreduce.textsearch;

import java.io.IOException;

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
import com.bit2017.mapreduce.topn.TopN;

public class SearchDocs {
	public static class MyMapper extends Mapper<Text, Text, Text, LongWritable> {
		private LongWritable one = new LongWritable(1L);
		private Text words = new Text();

		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			CharSequence c = conf.get("data");

			String line = value.toString();

			// StringTokenizer token = new StringTokenizer(line,
			// "\r\n\t,|()<>''.:");
			if (line.contains(c)) {
				words.set(line);
				context.write(words, one);
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
		conf.set("data", args[2]);

		Job job = new Job(conf, "SearchDocs");
		job.setJarByClass(SearchDocs.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);

		if (job.waitForCompletion(true) == false) {
			return;
		}

		Configuration conf2 = new Configuration();
		Job job2 = new Job(conf2, "TopN");

		job2.setJarByClass(TopN.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(LongWritable.class);

		job2.setMapperClass(TopN.MyMapper.class);
		job2.setReducerClass(TopN.MyReducer.class);

		job2.setInputFormatClass(KeyValueTextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/topN"));
		job2.getConfiguration().setInt("topN", 10);

		job2.waitForCompletion(true);

	}
}
