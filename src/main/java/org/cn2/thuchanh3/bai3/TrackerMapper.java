package org.cn2.thuchanh3.bai3;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TrackerMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
  private final Text dataKey = new Text("CO");
  private final DoubleWritable val = new DoubleWritable();

  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String line = value.toString();

    String[] parts = line.split(",");

    if (parts.length < 3) {
      return;
    }

    String type = parts[1].trim();

    if (type.equalsIgnoreCase("CO")) {
      try {
        double parsedVal = Double.parseDouble(parts[2].trim());
        val.set(parsedVal);

        context.write(dataKey, val);
      } catch (NumberFormatException e) {
        return;
      }
    }

  }
}
