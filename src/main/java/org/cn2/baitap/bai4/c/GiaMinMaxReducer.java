package org.cn2.baitap.bai4.c;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class GiaMinMaxReducer extends Reducer<Text, DoubleWritable, Text, Text> {
  private Text result = new Text();

  @Override
  public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
    double min = Double.POSITIVE_INFINITY;
    double max = Double.NEGATIVE_INFINITY;
    for (DoubleWritable val: values) {
      min = Math.min(val.get(), min);
      max = Math.max(val.get(), max);
    }
    result.set("Min: " + min + "; Max: " + max);
    context.write(key, result);
  }
}
