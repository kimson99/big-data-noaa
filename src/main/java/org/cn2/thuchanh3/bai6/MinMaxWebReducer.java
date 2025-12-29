package org.cn2.thuchanh3.bai6;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MinMaxWebReducer extends Reducer<Text, Text, Text, Text> {
  String globalMaxUrl = "";
  int globalMaxTime = Integer.MIN_VALUE;

  String globalMinUrl = "";
  int globalMinTime = Integer.MAX_VALUE;

  @Override
  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    for (Text val : values) {
      String[] parts = val.toString().split("\\$");

      if (parts.length < 3) continue;

      String type = parts[0];
      String url = parts[1];


      int time = Integer.parseInt(parts[2]);

      if (type.equals("Max")) {
        if (time > globalMaxTime) {
          globalMaxTime = time;
          globalMaxUrl = url;
        }
      } else if (type.equals("Min")) {
        if (time < globalMinTime) {
          globalMinTime = time;
          globalMinUrl = url;
        }
      }
    }

    context.write(new Text("WEBSITE_XEM_NHIEU_NHAT"), new Text(globalMaxUrl + "\t" + globalMaxTime + " phut"));
    context.write(new Text("WEBSITE_XEM_IT_NHAT"), new Text(globalMinUrl + "\t" + globalMinTime + " phut"));
  }
}
