package org.cn2.trend.global;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.log4j.BasicConfigurator;
import org.cn2.HadoopConnection;
import org.cn2.TableManager;

public class GlobalTrendDriver {
  public static void main(String[] args) throws Exception {
    BasicConfigurator.configure();

    Configuration conf = new HadoopConnection().init();

    // HBase
    final TableName tableName = TableName.valueOf("global_trend");
    TableManager tableManager = new TableManager(conf);
    final String tableFamilyName = "data";
    tableManager.recreateIfExist(tableName, tableFamilyName);

    // Job Setting
    Job job = Job.getInstance(conf, "Global Weather Trend");
    job.setJarByClass(GlobalTrendDriver.class);

    job.setMapperClass(GlobalTrendMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(DoubleWritable.class);

    String inputPathStr = "hdfs://namenode:9000/station/";
    Path inputPath = new Path(inputPathStr);

    job.setInputFormatClass(CombineTextInputFormat.class);
    CombineTextInputFormat.setMaxInputSplitSize(job, 134217728); // 128MB
    FileInputFormat.addInputPath(job, inputPath);

    job.getConfiguration().set("conf.column.family", tableFamilyName);

    TableMapReduceUtil.initTableReducerJob(
      tableName.toString(),
      GlobalTrendReducer.class,
      job
    );
    TableMapReduceUtil.initTableReducerJob(
      tableName.toString(),
      GlobalTrendReducer.class,
      job
    );

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
