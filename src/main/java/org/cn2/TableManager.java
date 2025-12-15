package org.cn2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class TableManager {
  private Configuration conf;
  public TableManager(Configuration _conf) {
    this.conf = _conf;
  }

  public void recreateIfExist( TableName tableName, String familyName) throws IOException {
    Connection connection = ConnectionFactory.createConnection(this.conf);
    Admin admin = connection.getAdmin();

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
    tableDescriptor.addFamily(new HColumnDescriptor(familyName));

    admin.createTable(tableDescriptor);
    admin.close();
    connection.close();
  }
}
