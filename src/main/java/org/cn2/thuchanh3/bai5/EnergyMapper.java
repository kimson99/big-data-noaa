package org.cn2.thuchanh3.bai5;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class EnergyMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
  private final Text dataKey = new Text("Total Revenue");
  private final DoubleWritable val = new DoubleWritable();

  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    String line = value.toString();

    String[] parts = line.split(",");

    if (parts.length < 2) {
      return;
    }

    try {
      double kwh = Double.parseDouble(parts[1].trim());
      double billAmount = calculateBill(kwh);
      val.set(billAmount);
      context.write(dataKey, val);
    } catch (NumberFormatException e) {
      //
    }

  }

  private double calculateBill(double kwh) {
    double total = 0;

    // Bậc 1: 0 - 100 (Giá 1.734)
    if (kwh <= 100) {
      return kwh * 1734;
    }
    total += 100 * 1734;
    kwh -= 100;

    // Bậc 2: 101 - 200 (Giá 2.014)
    if (kwh <= 100) {
      return total + (kwh * 2014);
    }
    total += 100 * 2014;
    kwh -= 100;

    // Bậc 3: 201 - 300 (Giá 2.536)
    if (kwh <= 100) {
      return total + (kwh * 2536);
    }
    total += 100 * 2536;
    kwh -= 100;

    // Bậc 4: > 301 (Giá 2.834)
    total += kwh * 2834;

    return total;
  }
}
