package dataexpo.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import dataexpo.Airline;
import dataexpo.DateKey;

import java.io.IOException;
public class DelayCountMapperWithDateKey extends  
											Mapper<LongWritable, Text, DateKey, IntWritable> {
  private final static IntWritable one = new IntWritable(1);
  private DateKey outputKey = new DateKey();
  @Override
  public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {
	if(key.get() == 0) return;  
    Airline al = new Airline(value);
    if (al.isDepartureDelayAvailable()) { 
      if (al.getDepartureDelayTime() > 0) { 
        outputKey.setYear("D," + al.getYear());
        outputKey.setMonth(al.getMonth());
        context.write(outputKey, one); //D,1988,1 1,1,1,1....
      } else if (al.getDepartureDelayTime() == 0) {
        context.getCounter(DelayCounters.scheduled_departure).increment(1);
      } else if (al.getDepartureDelayTime() < 0) {
        context.getCounter(DelayCounters.early_departure).increment(1);
      }
    } else {
      context.getCounter(DelayCounters.not_available_departure).increment(1);
    }
    if (al.isArriveDelayAvailable()) {
      if (al.getArriveDelayTime() > 0) {
        outputKey.setYear("A," + al.getYear());
        outputKey.setMonth(al.getMonth());
        context.write(outputKey, one);
      } else if (al.getArriveDelayTime() == 0) {
        context.getCounter(
          DelayCounters.scheduled_arrival).increment(1);
      } else if (al.getArriveDelayTime() < 0) {
        context.getCounter(DelayCounters.early_arrival).increment(1);
      }
    } else {
      context.getCounter(DelayCounters.not_available_arrival).increment(1);
    }
  }
}