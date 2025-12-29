package org.cn2.thuchanh3.bai4;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class GeometricMeanReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
  private final DoubleWritable result = new DoubleWritable();

  @Override
  public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
    double sum = 0;
    int count = 0;
    for (DoubleWritable val: values) {
      sum += val.get();
      count+=1;
    }
    if (count > 0) {
      double avg = sum / count;
      double geoMean = Math.exp(avg);
      result.set(geoMean);
      context.write(key, result);
    }
  }
}
