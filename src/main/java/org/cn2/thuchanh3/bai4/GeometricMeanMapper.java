package org.cn2.thuchanh3.bai4;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GeometricMeanMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
  private final Text dataKey = new Text("Geo Mean");
  private final DoubleWritable val = new DoubleWritable();

  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String line = value.toString();

    if (line.isEmpty()) {
      return;
    }

    try {
      double parsedVal = Double.parseDouble(line);

      if (parsedVal > 0) {
        double logVal = Math.log(parsedVal);
        val.set(logVal);
        context.write(dataKey, val);
      }
    } catch (NumberFormatException e) {
      return;
    }
  }
}
