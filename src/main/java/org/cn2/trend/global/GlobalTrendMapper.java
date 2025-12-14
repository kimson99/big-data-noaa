package org.cn2.trend.global;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WeatherMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
  @Override
  public void map(LongWritable key, Text value, Context context) {
    String line = value.toString();

    String[] parts = line.split(",");

    String id = parts[0];
    String date = parts[1];
    String element = parts[2];
    String dataValue = parts[3];
//    String mFlag = parts[4];
    String qFlag = parts[5];
//    String sFlag = parts[6];

    if (!element.equals("TMIN") && !element.equals("TMAX")) {
      return;
    }
    // Skip if quality flag is not empty
    if (!qFlag.isEmpty()) {
      return;
    }

    try {
      double temp = Double.POSITIVE_INFINITY;
      try {
        temp = Double.parseDouble(dataValue);
      } catch (Exception e) {
        //
      }

      context.write(new Text(id), new DoubleWritable(temp));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
