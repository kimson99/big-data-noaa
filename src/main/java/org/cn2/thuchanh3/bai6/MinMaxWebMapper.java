package org.cn2.thuchanh3.bai6;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MinMaxWebMapper extends Mapper<LongWritable, Text, Text, Text> {
  private String localMaxUrl = "";
  private int localMaxTime = Integer.MIN_VALUE;

  private String localMinUrl = "";
  private int localMinTime = Integer.MAX_VALUE;

  private final Text dataKey = new Text("Global Stats");
  private final IntWritable val = new IntWritable();

  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String line = value.toString();

    String[] parts = line.split("\\s+|,");

    if (parts.length < 2) {
      return;
    }

    String url = parts[0].trim();
    int watchTime;
    try {
       watchTime = Integer.parseInt((parts[1].trim()));
    } catch (NumberFormatException e) {
      return;
    }

    if (watchTime > localMaxTime) {
      localMaxTime = watchTime;
      localMaxUrl = url;
    }

    if (watchTime < localMinTime) {
      localMinTime = watchTime;
      localMinUrl = url;
    }
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    if (localMaxTime != Integer.MIN_VALUE) {
      context.write(dataKey, new Text("Max$" + localMaxUrl + "$" + localMaxTime));
    }

    if (localMinTime != Integer.MAX_VALUE) {
      context.write(dataKey, new Text("Min$" + localMinUrl + "$" + localMinTime));
    }
  }
}
