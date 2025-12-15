package org.cn2.trend.global;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;

public class GlobalTrendReducer extends TableReducer<Text, DoubleWritable, ImmutableBytesWritable> {

  @Override
  public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
    double max = Double.NEGATIVE_INFINITY;
    double min = Double.POSITIVE_INFINITY;
    int count = 0;
    double sum = 0;
    double mean = 0.0;
    double M2 = 0.0;

    for (DoubleWritable val : values) {
      double temp = val.get();
      if (!Double.isInfinite(temp)) {
        max = Math.max(max, temp);
        min = Math.min(min, temp);
        sum += temp;
        count++;

        // Welford's Update
        double delta = temp - mean;
        mean += delta / count;
        double delta2 = temp - mean;
        M2 += delta * delta2;
      }
    }

    if (count > 0) {
      double rawVariance = M2 / count;

      double finalMax = max / 10.0;
      double finalMin = min / 10.0;
      double finalAvg = (sum / count) / 10.0;
      double finalVar = rawVariance / 100.0;

      byte[] rowKey = Bytes.toBytes(key.toString());
      Put put = new Put(rowKey);

      // Define the Column Family
      final String familyName = "data";
      byte[] cf = Bytes.toBytes(familyName);

      // Add Columns (Family, Qualifier, Value)
      String maxStr = String.format("%.2f", finalMax);
      String minStr = String.format("%.2f", finalMin);
      String avgStr = String.format("%.2f", finalAvg);
      String varianceStr = String.format("%.2f", finalVar);

      put.addColumn(cf, Bytes.toBytes("max"), Bytes.toBytes(maxStr));
      put.addColumn(cf, Bytes.toBytes("min"), Bytes.toBytes(minStr));
      put.addColumn(cf, Bytes.toBytes("avg"), Bytes.toBytes(avgStr));
      put.addColumn(cf, Bytes.toBytes("var"), Bytes.toBytes(varianceStr));
      // Write to Context
      context.write(new ImmutableBytesWritable(rowKey), put);
    }
  }
}
