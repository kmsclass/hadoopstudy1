package test0126;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import dataexpo.DateKey;

public class DelayDistanceReducerWithDateKey extends Reducer<DateKey, IntWritable, DateKey, IntWritable> {
	private IntWritable result = new IntWritable();
	public void reduce(DateKey key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable value : values) {
			sum += value.get();
		}
		result.set(sum);
		context.write(key, result);
	}

}