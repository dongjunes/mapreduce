package com.bit2017.mapreduce.index;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class InvertedIndex1 {

	public static class MyMapper extends Mapper<Text, Text, Text, Text> {

		private Set<String> words = new HashSet<String>();
		private Text word = new Text();

		@Override
		protected void map(Text docId, Text contents, Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String line = contents.toString();
			StringTokenizer tokenizer = new StringTokenizer( line, "\r\n\t,|()<> ''.:" );
			words.clear();
			
			while( tokenizer.hasMoreTokens() ) {
				words.add( tokenizer.nextToken().toLowerCase() );
			}

			for( String w : words ) {
				word.set( w );
				context.write( word, docId );
			}		
		}
	}

	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		
		@Override
		protected void reduce(Text word, Iterable<Text> docIds,
				Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			StringBuilder sb = new StringBuilder();
			boolean isFirst = true;
			for( Text docId : docIds ) {
				if( isFirst == false ) {
					sb.append( "," );
				} else {
					isFirst = false;
				}
				
				sb.append( docId.toString() );
			}
			
			context.write( word, new Text( sb.toString() ) );
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job( conf, "Inverted Index 1" );
		
		//Job Instance 초기화 작업
		job.setJarByClass( InvertedIndex1.class );
		
		//맵퍼 클래스 지정
		job.setMapperClass( MyMapper.class );
		//리듀서 클래스 지정
		job.setReducerClass( MyReducer.class);
		//리듀스 개수 지정
		job.setNumReduceTasks( 10 );
		
		//map 출력 키 타입
		job.setMapOutputKeyClass( Text.class );
		//map 출력 밸류 타입
		job.setMapOutputValueClass( Text.class );
		
		
		//reduce 출력 키 타입
		job.setOutputKeyClass( Text.class );
		//reduce 출력 밸류 타입
		job.setOutputValueClass( Text.class );
		
		//입력파일 포맷 지정(생략 가능)
		job.setInputFormatClass( KeyValueTextInputFormat.class );
		//출력파일 포맷 지정(생략 가능)
		job.setOutputFormatClass( TextOutputFormat.class );
		
		//입력 파일 이름 지정
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		//출력 디렉토리 지정
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//실행
		job.waitForCompletion( true );
	}

}
