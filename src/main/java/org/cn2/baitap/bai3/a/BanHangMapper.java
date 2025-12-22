package org.cn2.baitap.bai3.a;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;

public class BanHangMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String line = value.toString();

    String[] parts = line.split(" ");
    ArrayList<Integer> parsed = new ArrayList<>();
    for (String p: parts) {
      parsed.add(Integer.parseInt(p));
    }

    for (int i = 0; i < parsed.size(); i++) {
      for (int j = i + 1; j < parsed.size(); j++ ) {
        int p1 = parsed.get(i);
        int p2 = parsed.get(j);
        String pair = "";
        if (p1 < p2) {
          pair = p1 + "," + p2;
        } else {
          pair = p2 + "," + p1;
        }
        context.write(new Text(pair), new IntWritable(1));
      }
    }
  }
}
