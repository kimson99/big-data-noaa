package org.cn2.weather;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.log4j.BasicConfigurator;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class WeatherDriverHBase {
  public static void main(String[] args) throws Exception {
    BasicConfigurator.configure();
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.property.clientPort", "2181");
    conf.set("hbase.zookeeper.quorum", "zoo");

    String NAME_NODE = "hdfs://namenode:9000";
    conf.set("fs.defaultFS", NAME_NODE);
    String user = System.getProperty("user.name");
    conf.set("yarn.app.mapreduce.am.staging-dir", "hdfs://namenode:9000/tmp/hadoop-yarn/staging");
    conf.set("mapreduce.cluster.local.dir", "/home/" + user + "/hadoop_data/temp");

    // HBase
    // Establish connection to HBase
    Connection connection = ConnectionFactory.createConnection(conf);
    Admin admin = connection.getAdmin();
    TableName tableName = TableName.valueOf("weather_data");

    // Check if table exists
    if (admin.tableExists(tableName)) {
      System.out.println("Table exists. Deleting...");
      // Must disable before deleting
      if (admin.isTableEnabled(tableName)) {
        admin.disableTable(tableName);
      }
      admin.deleteTable(tableName);
    }

    // Re-create the table
    System.out.println("Creating new table...");
    HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);

    // Add the Column Family 'data' to match your Reducer code
    tableDescriptor.addFamily(new HColumnDescriptor("data"));

    admin.createTable(tableDescriptor);
    admin.close();
    connection.close();

    // Job Setting
    Job job = Job.getInstance(conf, "Station Weather HBase");
    job.setJarByClass(WeatherDriverHBase.class);

    job.setMapperClass(WeatherMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(DoubleWritable.class);

    String inputPathStr = "hdfs://namenode:9000/station/";
    Path inputPath = new Path(inputPathStr);

    job.setInputFormatClass(CombineTextInputFormat.class);
    CombineTextInputFormat.setMaxInputSplitSize(job, 134217728); // 128MB
    FileInputFormat.addInputPath(job, inputPath);

    String targetTableName = "weather_data";
    TableMapReduceUtil.initTableReducerJob(
        targetTableName,
        WeatherReducerHBase.class,
        job);

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
