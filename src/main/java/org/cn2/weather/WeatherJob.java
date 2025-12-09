package org.cn2.weather;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class WeatherJob {
  public static void main(String[] args) throws Exception {
    BasicConfigurator.configure();

    Configuration conf = new Configuration();

    Job job = Job.getInstance(conf, "Station Weather");
    job.setJarByClass(WeatherJob.class);
    job.setMapperClass(WeatherMapper.class);
    job.setReducerClass(WeatherReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(DoubleWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(WeatherStats.class);

    String inputPathStr = "hdfs://namenode:9000/data/";
    String outputPathStr = "hdfs://namenode:9000/weather_output";

    Path inputPath = new Path(inputPathStr);
    Path outputPath = new Path(outputPathStr);
    // ---------------------------------
    String NAME_NODE = "hdfs://namenode:9000";
    conf.set("fs.defaultFS", NAME_NODE);
    // Clean up output directory in HDFS if it exists
    FileSystem fs = FileSystem.get(conf);
    if (fs.exists(outputPath)) {
      fs.delete(outputPath, true);
    }

    FileInputFormat.addInputPath(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
