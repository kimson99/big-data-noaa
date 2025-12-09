package org.cn2.weather;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class WeatherReducer extends Reducer<Text, DoubleWritable, Text, WeatherStats> {

  @Override
  public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
    double max = Double.NEGATIVE_INFINITY;
    double min = Double.POSITIVE_INFINITY;
    int count = 0;
    double sum = 0;

    for (DoubleWritable val : values) {
      double temp = val.get();
      if (!Double.isInfinite(temp)) {
        max = Math.max(max, temp);
        min = Math.min(min, temp);
        sum += temp;
        count++;
      }
    }
    double avg = sum / count;
    WeatherStats weatherStats = new WeatherStats(max / 10.0, min / 10.0,  avg / 10.0);
    context.write(key, weatherStats);
  }
}
