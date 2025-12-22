package org.cn2.baitap.bai4.a;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;

public class GiaMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
  private Text dateKey = new Text();
  private final static IntWritable one = new IntWritable(1);
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

    Configuration conf = context.getConfiguration();
    String param = conf.get("limit.price", "0");

    Double minPrice = Double.parseDouble(param);

    String name = parts[0].replaceAll("\"", "").trim();
    String price = parts[1].replaceAll("\"", "").trim();
    String unit = parts[2].replaceAll("\"", "").trim();
    String date = parts[3].replaceAll("\"", "").trim();

    if (Double.parseDouble(price) > minPrice) {
      dateKey.set(date);
      context.write(dateKey, one);
    }

  }
}
