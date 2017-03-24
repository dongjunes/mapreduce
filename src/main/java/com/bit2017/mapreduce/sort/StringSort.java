package com.bit2017.mapreduce.sort;

import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class StringSort {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "String Sort");

		// job init
		job.setJarByClass(StringSort.class);

		// mapper 지정
		job.setMapperClass(Mapper.class);

		// reducer 지정
		job.setReducerClass(Reducer.class);

		// 맵출력 키 타입
		job.setMapOutputKeyClass(Text.class);

		// 맵출력 타입지정
		job.setMapOutputValueClass(Text.class);

		// 처리 결과 출력 키 타입(리듀스)
		job.setOutputKeyClass(Text.class);

		// 처리 결과 출력 밸류 타입(리듀스)
		job.setOutputValueClass(Text.class);

		// 입력 파일포멧 지정(생략가능
		job.setInputFormatClass(KeyValueTextInputFormat.class);

		// 출력 파일포멧 지정(생략가능
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		// 입력파일포멧 이름지정
		FileInputFormat.addInputPath(job, new Path(args[0]));

		// 출력디렉토리 지정,압축
		SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));
		SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);

		// 실행
		job.waitForCompletion(true);
	}

}
