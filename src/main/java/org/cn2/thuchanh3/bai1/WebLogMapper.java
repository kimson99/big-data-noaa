package org.cn2.thuchanh3.bai1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class WebLogMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
  private HashMap<String, Integer> urlStateMap;

  private final Text dataKey = new Text();
  private final IntWritable timeCount = new IntWritable();

  @Override
  protected void setup(Context context) {
    urlStateMap = new HashMap<>();
  }

  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String line = value.toString();

    String[] parts = line.split(",");

    if (parts.length < 2) {
      return;
    }

    String url = parts[0];
    int count;

    try {
      count = Integer.parseInt(parts[1].trim());
    } catch (NumberFormatException e) {
      return; // Skip lines with bad numbers
    }

    if (urlStateMap.containsKey(url)) {
      int currentSum = urlStateMap.get(url);
      urlStateMap.put(url, currentSum + count);
    } else {
      urlStateMap.put(url, count);
    }
  }

  @Override
  protected  void cleanup(Context context) throws IOException, InterruptedException {
    for (Map.Entry<String, Integer> entry : urlStateMap.entrySet()) {
      dataKey.set(entry.getKey());
      timeCount.set(entry.getValue());

      context.write(dataKey, timeCount);
    }
  }
}
