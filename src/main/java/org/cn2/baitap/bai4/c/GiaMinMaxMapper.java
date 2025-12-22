package org.cn2.baitap.bai4.c;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;

public class GiaMinMaxMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
  private Text nameKey = new Text();
  private DoubleWritable priceResult = new DoubleWritable(0);
  @Override
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    // ten,gia,donvitinh,ngaycapnhat
    String line = value.toString();

    String[] parts = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
    ArrayList<Integer> parsed = new ArrayList<>();
    // Assume input without header
    if (parts.length < 4) {
      return;
    }

    String name = parts[0].replaceAll("\"", "").trim();
    String price = parts[1].replaceAll("\"", "").trim();
    String unit = parts[2].replaceAll("\"", "").trim();
    String date = parts[3].replaceAll("\"", "").trim();

    double parsedPrice = Double.parseDouble(price);
    nameKey.set(name);
    priceResult.set(parsedPrice);
    context.write(nameKey, priceResult);

  }
}
