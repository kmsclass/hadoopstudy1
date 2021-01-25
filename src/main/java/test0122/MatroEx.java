package test0122;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import dataexpo.mapreduce.DelayCountReducer;

public class MatroEx {
	public static void main(String[] args) throws Exception {
		String in = "infile/서울시 지하철 호선별 역별 시간대별 승하차 인원 정보.csv";
		String out = "outfile/matro2";
		Configuration conf = new Configuration();
		Job job = new Job(conf,"MatroEx");
		FileInputFormat.addInputPath(job,new Path(in));
		FileOutputFormat.setOutputPath(job, new Path(out));
		job.setJarByClass(MatroEx.class);
		job.setMapperClass(MatroMapper.class);
		job.setReducerClass(DelayCountReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);		
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(new Path(out))) {
			hdfs.delete(new Path(out), true);
			System.out.println("기존 출력파일 삭제");
		}
		job.waitForCompletion(true);

	}

}
