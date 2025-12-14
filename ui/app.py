import streamlit as st
import plotly.express as px
import numpy as np

from station import load_station_data
from trend import load_global_trend_data

# 1. SETUP PAGE
st.set_page_config(layout="wide", page_title="NOAA Weather Dashboard")

# Initialize Session State for simple navigation
if "current_view" not in st.session_state:
  st.session_state["current_view"] = "map" # Options: 'map', 'detail'
if "selected_station" not in st.session_state:
  st.session_state["selected_station"] = None

# 2. VIEW FUNCTIONS
def view_map(df):
  st.title("NOAA Weather Dashboard 🌦️")

  # Quick Summary Metrics
  c1, c2, c3 = st.columns(3)
  c1.metric("Active Stations", len(df))
  c2.metric("Global High", f"{df['max_temp'].max()}°C")
  c3.metric("Global Low", f"{df['min_temp'].min()}°C")

  # The Interactive Map
  fig = px.scatter_map(
    df,
    lat="lat",
    lon="lon",
    hover_name="name",
    hover_data={"avg_temp": True, "station_id": True},
    color="avg_temp",
    color_continuous_scale="RdYlBu_r",
    zoom=1,
    height=600
  )
  fig.update_layout(mapbox_style="open-street-map", margin={"r":0,"t":0,"l":0,"b":0})

  # Capture Click
  event = st.plotly_chart(fig, on_select="rerun", selection_mode="points", width="stretch")

  # Navigation Logic
  if event and event["selection"]["points"]:
    idx = event["selection"]["points"][0]["point_index"]
    # Save selection to state
    st.session_state["selected_station"] = df.iloc[idx].to_dict()
    st.session_state["current_view"] = "detail"
    st.rerun()

  # Global Trend
  st.divider()
  st.subheader("Global Temperature Trend")

  df_global_trend_data = load_global_trend_data()
  if not df_global_trend_data.empty:
    df_global_trend_data["Year"] = df_global_trend_data["Year"].astype(str)
    st.line_chart(
      df_global_trend_data.set_index("Year")[["Max", "Average", "Min"]],
      color=["#FF4B4B", "#FFA500", "#1E90FF"] # Red (Max), Orange (Avg), Blue (Min)
    )
  else:
    st.warning("Global trend data not available")

  # Standard Deviation
  st.divider()
  df_global_trend_data["stability"] = np.sqrt(df_global_trend_data["Variance"])
  st.subheader("Weather Instability (Standard Deviation)")
  st.caption("Higher values mean more extreme temperature swings within that year.")
  st.line_chart(df_global_trend_data.set_index("Year")["stability"])


def view_detail():
  station = st.session_state["selected_station"]

  # Back Button
  if st.button("← Back to Map"):
    st.session_state["current_view"] = "map"
    st.rerun()

  st.title(f"{station['name']}")
  st.caption(f"Station ID: {station['station_id']}")

  # Detail Metrics
  col1, col2, col3 = st.columns(3)
  col1.metric("Average", f"{station['avg_temp']:.1f}°C")
  col2.metric("Max Recorded", f"{station['max_temp']:.1f}°C", delta=f"{station['max_temp'] - station['avg_temp']:.1f} var")
  col3.metric("Min Recorded", f"{station['min_temp']:.1f}°C", delta=f"{station['min_temp'] - station['avg_temp']:.1f} var")

  st.divider()

  # Trend Section (Placeholder)
  st.subheader("Temperature Trend Analysis")
  st.info("🚧 No historical data available yet. This chart will display the yearly trend once the time-series MapReduce job is connected.")

  # Visual placeholder to show what it WILL look like
  # st.line_chart(...)

# 4. MAIN EXECUTION
df_station_data = load_station_data()

if not df_station_data.empty:
  if st.session_state["current_view"] == "map":
    view_map(df_station_data)
  elif st.session_state["current_view"] == "detail":
    view_detail()
else:
  st.warning("No data found. Ensure HBase is running and tables ('weather_data', 'station') are populated.")
