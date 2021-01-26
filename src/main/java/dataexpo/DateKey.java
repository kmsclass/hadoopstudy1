package dataexpo;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

//맵리듀스의 키로 사용되는 클래스 : WritableComparable 인터페이스의 구현클래스임.
//
public class DateKey implements WritableComparable<DateKey> {
  private String year;
  private Integer month;

  public DateKey() {  }
  public DateKey(String year, Integer date) {
    this.year = year;
    this.month = date;
  }
  //getter, setter, toString
  public String getYear() {
    return year;
  }
  public void setYear(String year) {
    this.year = year;
  }
  public Integer getMonth() {
    return month;
  }
  public void setMonth(Integer month) {
    this.month = month;
  }
  @Override
  public String toString() { //1988,1
    return (new StringBuilder()).append(year).append(",").append(month).toString();
  }
//오버라이딩
  @Override
  public void readFields(DataInput in) throws IOException {
    year = WritableUtils.readString(in);
    month = in.readInt();
  }
  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeString(out, year);
    out.writeInt(month);
  }
  @Override
  public int compareTo(DateKey key) { //정렬관련 메서드
    int result = year.compareTo(key.year);
    if (0 == result) { //같은 년도 데이터
      result = month.compareTo(key.month);
    }
    return result;
  }
}
