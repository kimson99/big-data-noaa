package org.cn2.baitap.bai3.a;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class BanHangDriver {
  public static void main(String[] args) throws Exception {
    BasicConfigurator.configure();
    Configuration conf = new Configuration();
    String NAME_NODE = "hdfs://namenode:9000";
    conf.set("fs.defaultFS", NAME_NODE);
    String user = System.getProperty("user.name");
    conf.set("yarn.app.mapreduce.am.staging-dir", "hdfs://namenode:9000/tmp/hadoop-yarn/staging");
    conf.set("mapreduce.cluster.local.dir", "/home/" + user + "/hadoop_data/temp");

    Job job = Job.getInstance(conf, "Retail Job A");
    job.setJarByClass(BanHangDriver.class);
    job.setMapperClass(BanHangMapper.class);
    job.setReducerClass(BanHangReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    String inputPathStr = "hdfs://namenode:9000/baitap/retail.dat.gz";
    String outputPathStr = "hdfs://namenode:9000/baitap_output/bai3/a";

    Path inputPath = new Path(inputPathStr);
    Path outputPath = new Path(outputPathStr);
    // ---------------------------------

    // Clean up output directory in HDFS if it exists
    FileSystem fs = FileSystem.get(conf);
    if (fs.exists(outputPath)) {
      fs.delete(outputPath, true);
    }

    job.setInputFormatClass(CombineTextInputFormat.class);
    // 128 * 1024 * 1024 = 134217728 bytes
    CombineTextInputFormat.setMaxInputSplitSize(job, 134217728);

    FileInputFormat.addInputPath(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
