package org.cn2.thuchanh3.bai2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SupermarketMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
  private long localSumRevenue;

  private final Text dataKey = new Text("Total Revenue");
  private final LongWritable revenue = new LongWritable();

  @Override
  protected void setup(Context context) {
    localSumRevenue = 0;
  }

  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String line = value.toString();

    String[] parts = line.split(",");

    if (parts.length < 4) {
      return;
    }


    try {
      long price = Integer.parseInt(parts[2].trim());
      int quantity = Integer.parseInt(parts[3].trim());

      long lineTotal = price * quantity;

      localSumRevenue += lineTotal;
    } catch (NumberFormatException e) {
      return;
    }
  }

  @Override
  protected  void cleanup(Context context) throws IOException, InterruptedException {
    revenue.set(localSumRevenue);
    context.write(dataKey, revenue);
  }
}
