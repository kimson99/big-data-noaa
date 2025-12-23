# trend_station.py
import streamlit as st
import pandas as pd
from hbase import get_connection

@st.cache_data(ttl=600)
def load_station_trend_data(station_id: str) -> pd.DataFrame:
    """
    Fetches yearly temperature trends for a specific station.
    RowKey format: STATION_ID#YEAR (e.g., 'ASN00003003#1950')
    Columns: b'data:avg', b'data:max', b'data:min'
    """
    if not station_id:
        return pd.DataFrame()
    
    try:
        hbase_connection = get_connection()
        table = hbase_connection.table('station_year_trend')

        prefix = f"{station_id}#".encode('utf-8')
        rows = []
        for key, data in table.scan(row_prefix=prefix):
            key_str = key.decode('utf-8')
            parts = key_str.split('#')
            if len(parts) != 2:
                continue
            try:
                year = int(parts[1])
            except ValueError:
                continue

            #  Fix: Use correct column names as in your HBase schema
            avg_val = float(data.get(b'data:avg', b'0').decode())
            max_val = float(data.get(b'data:max', b'0').decode())
            min_val = float(data.get(b'data:min', b'0').decode())

            rows.append({
                "Year": year,
                "Average": avg_val,
                "Max": max_val,
                "Min": min_val,
            })
        hbase_connection.close()

        df = pd.DataFrame(rows)
        if not df.empty:
            df = df.sort_values("Year").reset_index(drop=True)
        return df

    except Exception as e:
        st.error(f" Station Trend Data Error (ID: {station_id}): {e}")
        return pd.DataFrame()


#  NEW: Load ALL station-year trends (for anomaly detection & comparison)
@st.cache_data(ttl=600)
def load_all_station_trends() -> pd.DataFrame:
    """
    Scans entire 'station_year_trend' table.
    Returns DataFrame with columns: ['station_id', 'Year', 'Average', 'Min', 'Max']
    """
    try:
        hbase_connection = get_connection()
        table = hbase_connection.table('station_year_trend')

        rows = []
        for key, data in table.scan():
            key_str = key.decode('utf-8')
            parts = key_str.split('#')
            if len(parts) != 2:
                continue
            station_id, year_str = parts
            try:
                year = int(year_str)
            except ValueError:
                continue

            avg_val = float(data.get(b'data:avg', b'0').decode())
            max_val = float(data.get(b'data:max', b'0').decode())
            min_val = float(data.get(b'data:min', b'0').decode())

            rows.append({
                "station_id": station_id,
                "Year": year,
                "Average": avg_val,
                "Max": max_val,
                "Min": min_val,
            })
        hbase_connection.close()

        df = pd.DataFrame(rows)
        if not df.empty:
            df = df.sort_values(["station_id", "Year"]).reset_index(drop=True)
        return df

    except Exception as e:
        st.error(f" All Station Trends Scan Error: {e}")
        return pd.DataFrame()