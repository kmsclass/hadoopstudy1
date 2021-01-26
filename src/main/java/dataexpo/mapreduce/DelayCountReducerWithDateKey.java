package dataexpo.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import dataexpo.DateKey;

import java.io.IOException;


public class DelayCountReducerWithDateKey extends 
 											Reducer<DateKey, IntWritable, DateKey, IntWritable> {
	private MultipleOutputs<DateKey, IntWritable> mos;
	private DateKey outputKey = new DateKey();
	private IntWritable result = new IntWritable();
	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		mos = new MultipleOutputs<DateKey, IntWritable>(context);
	}
	public void reduce(DateKey key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		String[] colums = key.getYear().split(",");//D,1988
		outputKey.setYear(key.getYear().substring(2)); //1988
		outputKey.setMonth(key.getMonth());
		int sum = 0;
		for (IntWritable value : values) {
			sum += value.get();
		}
		result.set(sum);
		if (colums[0].equals("D")) {
			mos.write("departure", outputKey, result);
		} else {
			mos.write("arrival", outputKey, result);
		}
	}
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		mos.close();
	}
}