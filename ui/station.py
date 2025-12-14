import streamlit as st
import pandas as pd

from hbase import get_connection

@st.cache_data(ttl=600)
def load_station_data():
  """Fetches and joins weather stats with station metadata."""
  try:
    # Connect to HBase
    hbase_connection = get_connection()

    # Fetch Weather Stats (The MapReduce Output)
    weather_table = hbase_connection.table('weather_data')
    stats_data = []
    for key, data in weather_table.scan():
      stats_data.append({
        "station_id": key.decode('utf-8'),
        "avg_temp": float(data.get(b'data:avg', b'0')),
        "max_temp": float(data.get(b'data:max', b'0')),
        "min_temp": float(data.get(b'data:min', b'0'))
      })
    df_stats = pd.DataFrame(stats_data)

    # Fetch Station Metadata (Coordinates)
    station_table = hbase_connection.table('station')
    meta_data = []
    for key, data in station_table.scan():
      meta_data.append({
        "station_id": key.decode('utf-8'),
        "name": data.get(b'metadata:name', b'Unknown').decode('utf-8'),
        "lat": float(data.get(b'metadata:lat', 0)),
        "lon": float(data.get(b'metadata:lon', 0)),
      })
    df_meta = pd.DataFrame(meta_data)

    hbase_connection.close()

    # oin them (Inner Join)
    if not df_stats.empty and not df_meta.empty:
      return pd.merge(df_stats, df_meta, on="station_id", how="inner")

    return pd.DataFrame()

  except Exception as e:
    st.error(f"HBase Connection Error: {e}")
    return pd.DataFrame()
