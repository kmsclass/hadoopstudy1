package hdfs;

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class WriteHadoopFile1 {
	public static void main(String[] args) throws IOException {
		String filepath = "outfile/out.txt";
		Path pt = new Path(filepath);
		Configuration conf = new Configuration(); 
		FileSystem fs = FileSystem.get(conf);
		System.out.println("하둡에 저장할 문자를 입력하세요");
		Scanner scan = new Scanner(System.in); //표준입력
		//hdfs시스템에 저장하는 출력 스트림
		FSDataOutputStream out = fs.create(pt); //존재하지 않는 파일은 파일 생성. 존재하는 파일이면 파일에
		while(true) {
			String console = scan.nextLine();
			if(console.equals("exit")) break;
			out.writeUTF(console + "\n"); //문자열 등록.
		}
		out.flush();
		out.close();
	}
}
