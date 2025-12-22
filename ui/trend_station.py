import streamlit as st
import pandas as pd
from hbase import get_connection

@st.cache_data(ttl=600)
def load_station_trend_data(station_id):
  """Fetches yearly temperature trends for a specific station."""
  try:
    hbase_connection = get_connection()
    table = hbase_connection.table('station_year_trend')

    # Scan with prefix "STATION_ID#"
    # Example RowKey: ASN00003003#1950
    prefix = f"{station_id}#".encode('utf-8')
    
    rows = []
    for key, data in table.scan(row_prefix=prefix):
      # Extract Year from RowKey
      key_str = key.decode('utf-8')
      year = int(key_str.split('#')[1])

      rows.append({
        "Year": year,
        "Average": float(data.get(b'data:avg', b'0')),
        "Max": float(data.get(b'data:max', b'0')),
        "Min": float(data.get(b'data:min', b'0')),
      })

    hbase_connection.close()

    df = pd.DataFrame(rows)
    if not df.empty:
      df = df.sort_values("Year")

    return df

  except Exception as e:
    st.error(f"Station Trend Data Error: {e}")
    return pd.DataFrame()
