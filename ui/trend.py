import streamlit as st
import pandas as pd

from hbase import get_connection

@st.cache_data(ttl=600)
def load_global_trend_data():
  """Fetches global yearly temperature trends."""
  try:
    hbase_connection = get_connection()
    table = hbase_connection.table('global_trend')

    rows = []
    for key, data in table.scan():
      rows.append({
        "Year": int(key.decode('utf-8')), # RowKey is Year
        "Average": float(data.get(b'data:avg', b'0')),
        "Max": float(data.get(b'data:max', b'0')),
        "Min": float(data.get(b'data:min', b'0')),
        "Variance": float(data.get(b'data:var', b'0')),
      })

    hbase_connection.close()

    # Sort by Year so the line chart doesn't look like spaghetti
    df = pd.DataFrame(rows)
    if not df.empty:
      df = df.sort_values("Year")

    return df

  except Exception as e:
    st.error(f"Trend Data Error: {e}")
    return pd.DataFrame()
