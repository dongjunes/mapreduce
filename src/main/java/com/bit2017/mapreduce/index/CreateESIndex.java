package com.bit2017.mapreduce.index;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CreateESIndex {

	public static class ESIndexMapper extends Mapper<Text, Text, Text, Text> {

		private String baseURL = "";

		@Override
		protected void setup(Mapper<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			String hosts[] = context.getConfiguration().getStrings("ESServer");
			baseURL = "http://" + hosts[0] + ":9200/wikipedia/doc/";
		}

		@Override
		protected void map(Text docId, Text contents, Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// URLConnection 객체 생성
			URL url = new URL(baseURL + docId);
			HttpURLConnection urlCon = (HttpURLConnection) url.openConnection();
			urlCon.setDoOutput(true);
			urlCon.setRequestMethod("PUT");

			// json문자열 만들기
			String line = contents.toString().replace("\\", "\\\\").replace("\"", "\\\"");
			String json = "{\"body\":\"" + line + "\" }";

			// 데이터 보내기
			OutputStreamWriter out = new OutputStreamWriter(urlCon.getOutputStream());
			out.write(json);
			out.close();

			// 응답받기
			BufferedReader br = new BufferedReader(new InputStreamReader(urlCon.getInputStream()));
			StringBuilder sb = new StringBuilder();
			while ((line = br.readLine()) != null) {
				sb.append(line);
			}

			if (sb.indexOf("\"successful\":1,\"failed\":0") < 0) {
				context.getCounter("stats", "error docs").increment(1);
			} else {
				context.getCounter("stats", "success docs").increment(1);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Create ES Index");

		// Job Instance 초기화 작업
		job.setJarByClass(CreateESIndex.class);

		// 맵퍼 클래스 지정
		job.setMapperClass(ESIndexMapper.class);
		// 리듀스 개수 지정
		job.setNumReduceTasks(0);

		// map 출력 키 타입
		job.setMapOutputKeyClass(Text.class);
		// map 출력 밸류 타입
		job.setMapOutputValueClass(Text.class);

		// 입력파일 포맷 지정(생략 가능)
		job.setInputFormatClass(KeyValueTextInputFormat.class);

		// 입력 파일 이름 지정
		FileInputFormat.addInputPath(job, new Path(args[0]));
		// 출력 디렉토리 지정
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// ES Server지정
		job.getConfiguration().setStrings("ESServer", args[2]);

		// 실행
		job.waitForCompletion(true);
	}
}
