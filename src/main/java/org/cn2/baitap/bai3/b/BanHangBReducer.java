package org.cn2.baitap.bai3.b;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class BanHangBReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
  private DoubleWritable result = new DoubleWritable();
  private double totalCountA = 0;
  private String currentItemA = "";

  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    // Key format: "ItemA, Neighbor" (ví dụ: "25,*" hoặc "25,52")
    String[] parts = key.toString().split(",");
    String itemA = parts[0];
    String neighbor = parts[1];

    if (!itemA.equals(currentItemA)) {
      currentItemA = itemA;
      totalCountA = 0;
    }

    int sum = 0;
    for (IntWritable val : values) {
      sum += val.get();
    }

    // Order Inversion:
    if (neighbor.equals("*")) {
      totalCountA = sum;
    } else {
      double probability = sum / totalCountA;
      result.set(probability);
      // Output: "25,52"  0.25 (nghĩa là P(52|25) = 25%)
      context.write(key, result);
    }

    context.write(key, result);
  }
}
