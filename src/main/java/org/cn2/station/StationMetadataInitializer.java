package org.cn2.station;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.BasicConfigurator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class StationMetadataInitializer {

  private static final String FILE_PATH = "data/ghcnd-stations.csv"; // Path inside resources
  private static final int BATCH_SIZE = 1000; // Commit every 1000 rows

  public static void main(String[] args) throws IOException {
    BasicConfigurator.configure();
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.property.clientPort", "2181");
    conf.set("hbase.zookeeper.quorum", "zknode1");

    TableName tableName = TableName.valueOf("station");

    // Try-with-resources to ensure connections close automatically
    try (Connection connection = ConnectionFactory.createConnection(conf)) {

      Admin admin = connection.getAdmin();
      if (admin.tableExists(tableName)) {
        if (admin.isTableEnabled(tableName)) {
          admin.disableTable(tableName);
        }
        admin.deleteTable(tableName);
      }

      HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
      tableDescriptor.addFamily(new HColumnDescriptor("metadata"));
      admin.createTable(tableDescriptor);

      Table table = connection.getTable(tableName);
      System.out.println("Connected to HBase. Reading file...");
      loadData(table);
      System.out.println("Data loading complete!");
      admin.close();
    }
  }

  private static void loadData(Table table) throws IOException {
    // Load file from src/main/resources
    ClassLoader classLoader = StationMetadataInitializer.class.getClassLoader();
    InputStream inputStream = classLoader.getResourceAsStream(FILE_PATH);

    if (inputStream == null) {
      throw new IllegalArgumentException("File not found! Check src/main/resources/" + FILE_PATH);
    }

    try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
      String line;
      List<Put> batch = new ArrayList<>();
      int count = 0;

      while ((line = reader.readLine()) != null) {
        if (line.trim().isEmpty()) continue;

        try {
          String[] splitted =  line.split(",");
          if (splitted.length != 9) {
            continue;
          }
          String id = splitted[0].trim();
          String lat = splitted[1].trim();
          String lon = splitted[2].trim();


          String elevation = splitted[3].trim();
          String state = splitted[4].trim();
          String name = splitted[5].trim();

          // CREATE PUT
          // Row Key = Station ID
          Put put = new Put(Bytes.toBytes(id));

          // Add columns to 'info' family
          put.addColumn(Bytes.toBytes("metadata"), Bytes.toBytes("lat"), Bytes.toBytes(lat));
          put.addColumn(Bytes.toBytes("metadata"), Bytes.toBytes("lon"), Bytes.toBytes(lon));
          put.addColumn(Bytes.toBytes("metadata"), Bytes.toBytes("elevation"), Bytes.toBytes(elevation));
          put.addColumn(Bytes.toBytes("metadata"), Bytes.toBytes("state"), Bytes.toBytes(state));
          put.addColumn(Bytes.toBytes("metadata"), Bytes.toBytes("name"), Bytes.toBytes(name));

          batch.add(put);
          count++;

          if (batch.size() >= BATCH_SIZE) {
            table.put(batch);
            batch.clear();
            System.out.println("Inserted " + count + " rows...");
          }

        } catch (StringIndexOutOfBoundsException e) {
          System.err.println("Skipping malformed line: " + line);
        }
      }

      // Commit remaining rows
      if (!batch.isEmpty()) {
        table.put(batch);
        System.out.println("Inserted remaining " + batch.size() + " rows.");
      }
    }
  }
}
