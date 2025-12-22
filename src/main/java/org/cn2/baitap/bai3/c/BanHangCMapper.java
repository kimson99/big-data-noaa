package org.cn2.baitap.bai3.c;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

public class BanHangCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
  private final static IntWritable one = new IntWritable(1);
  private Text triplet = new Text();

  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String line = value.toString();

    String[] parts = line.split(" ");
    ArrayList<Integer> items = new ArrayList<>();
    for (String p: parts) {
      items.add(Integer.parseInt(p));
    }
    Collections.sort(items);

    int size = items.size();
    if (size < 3) return;

    for (int i = 0; i < size; i++) {
      for (int j = i + 1; j < size; j++) {
        for (int k = j + 1; k < size; k++) {

          int p1 = items.get(i);
          int p2 = items.get(j);
          int p3 = items.get(k);

          // Tạo Key: "A,B,C"
          triplet.set(p1 + "," + p2 + "," + p3);
          context.write(triplet, one);
        }
      }
    }
  }
}
