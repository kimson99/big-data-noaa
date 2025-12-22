package org.cn2.trend.station;

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

public class StationYearTrendDriver {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        // Init Hadoop/HBase config with correct Zookeeper host
        Configuration conf = new HadoopConnection().init();

        // HBase Table Setup
        final TableName tableName = TableName.valueOf("station_year_trend");
        TableManager tableManager = new TableManager(conf);
        final String tableFamilyName = "data";
        tableManager.recreateIfExist(tableName, tableFamilyName);

        // Job Setup
        Job job = Job.getInstance(conf, "Station Year Trend Analysis");
        job.setJarByClass(StationYearTrendDriver.class);

        job.setMapperClass(StationYearMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        // Input Path (Same as other jobs)
        String inputPathStr = "hdfs://namenode:9000/station/";
        Path inputPath = new Path(inputPathStr);

        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(job, 134217728); // 128MB
        FileInputFormat.addInputPath(job, inputPath);

        // Reducer Setup
        TableMapReduceUtil.initTableReducerJob(
                tableName.toString(),
                StationYearReducer.class,
                job);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
