package test0126;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import dataexpo.Airline;
import dataexpo.DateKey;

public class DelayDistanceMapperWithDateKey extends 
                                        Mapper<LongWritable, Text, DateKey, IntWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if (key.get() == 0) 	return;
		Airline al = new Airline(value);
		if (al.isDistanceAvailable()) {
			if (al.getDistance() > 0) {
				DateKey outputKey = new DateKey();
				IntWritable distance = new IntWritable(al.getDistance());
				outputKey.setYear(new StringBuilder().append(al.getYear()).toString());
				outputKey.setMonth(al.getMonth());
				context.write(outputKey, distance); 
			}
		}
	}
}