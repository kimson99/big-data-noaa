package org.cn2.trend.station;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;

public class StationYearReducer extends TableReducer<Text, DoubleWritable, ImmutableBytesWritable> {
    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
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

        if (count > 0) {
            double finalMax = max / 10.0;
            double finalMin = min / 10.0;
            double finalAvg = (sum / count) / 10.0;

            // Row Key: StationID#Year
            byte[] rowKey = Bytes.toBytes(key.toString());
            Put put = new Put(rowKey);

            byte[] cf = Bytes.toBytes("data");

            String maxStr = String.format("%.2f", finalMax);
            String minStr = String.format("%.2f", finalMin);
            String avgStr = String.format("%.2f", finalAvg);

            put.addColumn(cf, Bytes.toBytes("max"), Bytes.toBytes(maxStr));
            put.addColumn(cf, Bytes.toBytes("min"), Bytes.toBytes(minStr));
            put.addColumn(cf, Bytes.toBytes("avg"), Bytes.toBytes(avgStr));

            context.write(new ImmutableBytesWritable(rowKey), put);
        }
    }
}
