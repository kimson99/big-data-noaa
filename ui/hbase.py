import happybase

def get_connection():
  connection = happybase.Connection('hbase-master', port=9090)
  return connection
