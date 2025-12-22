package org.cn2.baitap.bai3.b;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;

public class BanHangBMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
  private final static IntWritable one = new IntWritable(1);
  private Text outputKey = new Text();

  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String line = value.toString();

    String[] parts = line.split(" ");
    ArrayList<String> items = new ArrayList<>();
    for (String p: parts) {
      items.add(p);
    }

    for (int i = 0; i < items.size(); i++) {
      String itemA = items.get(i);
      // Emit ("A, *", 1) để đếm tổng số lần A xuất hiện
      outputKey.set(itemA + ",*");
      context.write(outputKey, one);
      // Emit ("A, B", 1) cho tất cả các phần tử khác trong cùng giao dịch
      for (int j = 0; j < items.size(); j++ ) {
        if (i == j) continue;
        String itemB = items.get(j);

        outputKey.set(itemA + "," + itemB);
        context.write(outputKey, one);
      }
    }
  }
}
