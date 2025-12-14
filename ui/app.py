import streamlit as st
import happybase
import pandas as pd

# Connect to HBase
connection = happybase.Connection('hbase-master', port=9090)
table = connection.table('weather_data')

st.title("Weather Data Dashboard 🌦️")

# Fetch all data
data = []
for key, data_dict in table.scan():
  row_key = key.decode('utf-8')
  data.append({
    "Station ID": row_key,
    "Average": float(data_dict[b'data:avg'].decode('utf-8')),
    "Max": float(data_dict[b'data:max'].decode('utf-8')),
    "Min": float(data_dict[b'data:min'].decode('utf-8'))
  })

connection.close()

if data:
  df = pd.DataFrame(data)

  # Sidebar Filter
  station_list = ["All Stations"] + sorted(df["Station ID"].unique().tolist())
  selected_station = st.sidebar.selectbox("Select a Station:", station_list)

  if selected_station != "All Stations":
    # --- SINGLE STATION VIEW ---
    st.header(f"Station: {selected_station}")

    # 1. Get the specific row
    station_data = df[df["Station ID"] == selected_station].iloc[0]

    # 2. Reshape the data for the Column Chart
    # We want: Index=['Average', 'Max', 'Min'], Value=Temp
    # This makes the "Type" appear on the X-axis
    chart_df = pd.DataFrame({
      "Temperature": [station_data["Average"], station_data["Max"], station_data["Min"]]
    }, index=["Average", "Max", "Min"])

    # 3. Display Metrics Side-by-Side
    col1, col2, col3 = st.columns(3)
    col1.metric("Average", f"{station_data['Average']:.2f} °C")
    col2.metric("Max", f"{station_data['Max']:.2f} °C")
    col3.metric("Min", f"{station_data['Min']:.2f} °C")

    # 4. Column Chart
    st.subheader("Temperature Breakdown")
    st.bar_chart(chart_df)

  else:
    # --- ALL STATIONS VIEW ---
    st.header("All Stations Overview")

    # Calculate global stats
    st.metric("Global Average Temp", f"{df['Average'].mean():.2f} °C")

    # Show raw data
    st.dataframe(df)

    # For "All Stations", we usually keep Station ID on the X-axis
    # But we use bar_chart now instead of line_chart
    st.subheader("Comparison Across Stations")
    st.bar_chart(df.set_index("Station ID")[["Max", "Average", "Min"]])

else:
  st.warning("No data found in HBase table 'weather_data'")
