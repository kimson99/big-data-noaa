package org.cn2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class HadoopConnection {
  public Configuration init() {
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.property.clientPort", "2181");
    conf.set("hbase.zookeeper.quorum", "zknode1");

    String NAME_NODE = "hdfs://namenode:9000";
    conf.set("fs.defaultFS", NAME_NODE);
    String user = System.getProperty("user.name");
    conf.set("yarn.app.mapreduce.am.staging-dir", "hdfs://namenode:9000/tmp/hadoop-yarn/staging");
    conf.set("mapreduce.cluster.local.dir", "/home/" + user + "/hadoop_data/temp");

    return conf;
  }
}
