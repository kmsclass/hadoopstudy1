package dataexpo.mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import dataexpo.Airline;

public class ArrivalDelayCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private final static IntWritable outputValue = new IntWritable(1);
	private Text outputKey = new Text();

	public void map(LongWritable key, Text value, Context context) 
			                                       throws IOException, InterruptedException {
        if(key.get() == 0) return;
        //value : 한줄 정보 
        //Year,Month,DayofMonth,DayOfWeek,DepTime,CRSDepTime,ArrTime,CRSArrTime,UniqueCarrier,FlightNum,TailNum,ActualElapsedTime,CRSElapsedTime,AirTime,ArrDelay,DepDelay,Origin,Dest,Distance,TaxiIn,TaxiOut,Cancelled,CancellationCode,Diverted,CarrierDelay,WeatherDelay,NASDelay,SecurityDelay,LateAircraftDelay
        //1987,  10,    14,          3,     741,     730,      912,    849,       PS,          1451,       NA,      91,      79,      NA,           23,     11,      SAN,    SFO,  447,   NA,     NA,    0,       NA,      0,               NA,      NA,           NA,         NA,          NA        
		Airline air = new Airline(value);
		outputKey.set(air.getYear() + "," + air.getMonth()); //년도+월
		if (air.isArriveDelayAvailable() && air.getArriveDelayTime() > 0) { //도착지연된 비행기
			context.write(outputKey, outputValue);
		}
	}
}