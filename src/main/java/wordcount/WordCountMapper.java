package wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper <LongWritable,Text,Text,IntWritable>{
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	@Override
	public void map(LongWritable key, Text value, Context context) 
			                                      throws IOException, InterruptedException {
		//value : 입력되는 데이터. Text 형.
		//        write a book
		//StringTokenizer : 문자열 토큰화함.
		// 토큰 : 의미있는 문자열의 최소 단위.
		StringTokenizer itr = new StringTokenizer(value.toString()); //공백단위로 문자열을 분리.
		while(itr.hasMoreElements()) {
			word.set(itr.nextToken()); //book
			//맵퍼의 결과 데이터 => 리듀서의 입력 데이터
			context.write(word, one); 
			//read ,1 
			//a    ,1,1
            //book ,1,1
			//write,1
	    }
    }
}