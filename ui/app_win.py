import streamlit as st
import plotly.express as px
import numpy as np
import pandas as pd

from station import load_station_data
from trend import load_global_trend_data
from trend_station import load_station_trend_data, load_all_station_trends 

# === Helper functions giữ nguyên ===
def load_station_trend_data_for_year(year: str) -> pd.DataFrame:
    df_all = load_all_station_trends()
    if df_all.empty: return pd.DataFrame()
    df_stations = load_station_data()[["station_id", "name"]]
    df_merged = df_all.merge(df_stations, on="station_id", how="left")
    year_int = int(year)
    return df_merged[df_merged["Year"] == year_int][["station_id", "name", "Average"]].copy()

# 1. SETUP PAGE
st.set_page_config(layout="wide", page_title="NOAA Weather Dashboard")

if "current_view" not in st.session_state:
    st.session_state["current_view"] = "map"
if "selected_station" not in st.session_state:
    st.session_state["selected_station"] = None

# 2. VIEW FUNCTIONS

def view_map(df):
    st.title("NOAA Weather Dashboard 🌦️")

    # --- 1. Quick Summary Metrics ---
    c1, c2, c3 = st.columns(3)
    c1.metric("Active Stations", len(df))
    c2.metric("Global High", f"{df['max_temp'].max():.1f}°C")
    c3.metric("Global Low", f"{df['min_temp'].min():.1f}°C")

    # --- 2. The Interactive Map ---
    fig = px.scatter_mapbox(
        df, lat="lat", lon="lon", hover_name="name",
        hover_data={"avg_temp": True, "station_id": True},
        color="avg_temp", color_continuous_scale="RdYlBu_r",
        zoom=1, height=600
    )
    fig.update_layout(mapbox_style="open-street-map", margin={"r": 0, "t": 0, "l": 0, "b": 0})

    event = st.plotly_chart(fig, on_select="rerun", selection_mode="points", use_container_width=True)

    if event and event["selection"]["points"]:
        idx = event["selection"]["points"][0]["point_index"]
        st.session_state["selected_station"] = df.iloc[idx].to_dict()
        st.session_state["current_view"] = "detail"
        st.rerun()

    # Tải dữ liệu xu hướng một lần để dùng chung
    df_global_trend_data = load_global_trend_data()
    if not df_global_trend_data.empty:
        df_global_trend_data["Year_str"] = df_global_trend_data["Year"].astype(str)

    # --- 3. Global Temperature Trend (CHIỀU DỌC) ---
    st.divider()
    st.subheader("🌍 Global Temperature Trend")
    if not df_global_trend_data.empty:
        st.line_chart(df_global_trend_data.set_index("Year_str")[["Max", "Average", "Min"]],
                      color=["#FF4B4B", "#FFA500", "#1E90FF"])
    else:
        st.warning("Global trend data not available")

    # --- 4. Weather Instability (CHIỀU DỌC) ---
    st.divider()
    st.subheader("🌀 Weather Instability (Standard Deviation)")
    st.caption("Higher values mean more extreme temperature swings within that year.")
    if not df_global_trend_data.empty:
        df_global_trend_data["stability"] = np.sqrt(df_global_trend_data["Variance"])
        st.line_chart(df_global_trend_data.set_index("Year_str")["stability"])

   # --- 5. Anomaly Detection (Fixed & Enhanced) ---
    st.divider()
    st.subheader("🌡️ Anomaly Detection")

    if not df_global_trend_data.empty:
        # Lấy danh sách năm có dữ liệu toàn cầu
        available_years = [str(y) for y in sorted(df_global_trend_data["Year"].unique(), reverse=True)]
        selected_year_str = st.selectbox(
            "Select Year for Anomaly Analysis", 
            options=available_years, 
            key="anomaly_year_selector"
        )
        
        year_int = int(selected_year_str)
        
        # Lấy thông số Global (Trung bình và Độ lệch chuẩn) cho năm được chọn
        year_row = df_global_trend_data[df_global_trend_data["Year"] == year_int]
        
        if not year_row.empty:
            global_avg = year_row.iloc[0]["Average"]
            # Tính độ lệch chuẩn σ từ Variance (Biến thiên)
            global_std = np.sqrt(year_row.iloc[0]["Variance"]) if "Variance" in year_row.columns else 0

            # Tải dữ liệu chi tiết của từng trạm trong năm đó
            df_year_stations = load_station_trend_data_for_year(selected_year_str)

            if not df_year_stations.empty:
                # Tính độ lệch của từng trạm so với trung bình thế giới
                df_year_stations["deviation"] = df_year_stations["Average"] - global_avg
                
                # Định nghĩa bất thường (Anomaly): Lệch quá 2 lần độ lệch chuẩn (±2σ)
                threshold = 2 * global_std
                df_year_stations["is_anomaly"] = np.abs(df_year_stations["deviation"]) > threshold
                anomalies = df_year_stations[df_year_stations["is_anomaly"]]

                # Hiển thị chỉ số nhanh
                m1, m2, m3 = st.columns(3)
                m1.metric(f"Global Avg ({year_int})", f"{global_avg:.2f}°C")
                m2.metric("Global Std Dev (σ)", f"{global_std:.2f}°C")
                m3.metric("Anomalous Stations", len(anomalies))

                # BIỂU ĐỒ KẾT HỢP: Histogram + Box Plot
                # Box plot ở phía trên giúp thấy ngay các điểm chấm (outliers)
                fig_dist = px.histogram(
                    df_year_stations,
                    x="Average",
                    nbins=30,
                    marginal="box", 
                    title=f"Station Temp Distribution vs Global Mean ({year_int})",
                    labels={"Average": "Annual Avg Temp (°C)"},
                    color_discrete_sequence=['#636EFA']
                )
                
                # Thêm các đường giới hạn thống kê
                fig_dist.add_vline(x=global_avg, line_dash="solid", line_color="orange", annotation_text="Mean")
                fig_dist.add_vline(x=global_avg + threshold, line_dash="dash", line_color="red", annotation_text="+2σ (Hot)")
                fig_dist.add_vline(x=global_avg - threshold, line_dash="dash", line_color="red", annotation_text="-2σ (Cold)")

                st.plotly_chart(fig_dist, use_container_width=True)

                # Hiển thị danh sách các trạm bất thường nếu có
                if not anomalies.empty:
                    with st.expander(f"⚠️ View {len(anomalies)} Anomalous Stations Details"):
                        st.dataframe(
                            anomalies[["station_id", "name", "Average", "deviation"]]
                            .sort_values("deviation", key=abs, ascending=False)
                            .reset_index(drop=True)
                            .style.format({"Average": "{:.2f}", "deviation": "{:+.2f}"})
                        )
                else:
                    st.success(f"✅ No statistical anomalies found for the year {year_int}.")
            else:
                st.info(f"No station-specific data found for {year_int} in HBase.")
        else:
            st.error(f"Global summary for {year_int} is missing.")
    else:
        st.warning("Global trend data is empty. Cannot perform anomaly detection.")

    # --- 6. Multi-Station Comparison (LUÔN HIỂN THỊ) ---
    st.divider()
    st.subheader("📊 Multi-Station Comparison")
    
    df["label"] = df["station_id"] + " – " + df["name"]
    selected_labels = st.multiselect(
        "Select up to 5 stations to compare trends:",
        df["label"].tolist(),
        max_selections=5,
        key="main_compare_select"
    )

    if selected_labels:
        selected_ids = [label.split(" – ")[0] for label in selected_labels]
        all_trends = []
        for sid in selected_ids:
            df_trend_single = load_station_trend_data(sid)
            if not df_trend_single.empty:
                df_trend_single["Station"] = sid
                all_trends.append(df_trend_single)

        if all_trends:
            combined = pd.concat(all_trends, ignore_index=True)
            combined["Year"] = combined["Year"].astype(str)
            fig_comp = px.line(
                combined, 
                x="Year", 
                y="Average", 
                color="Station", 
                markers=True,
                title="Historical Temperature Comparison"
            )
            fig_comp.update_layout(yaxis_title="Avg Temp (°C)")
            st.plotly_chart(fig_comp, use_container_width=True)

def view_detail():
    station = st.session_state["selected_station"]
    if st.button("← Back to Global Map"):
        st.session_state["current_view"] = "map"
        st.rerun()

    st.title(f"📍 {station['name']}")
    st.caption(f"ID: {station['station_id']}")
    # ... (giữ nguyên phần code view_detail của bạn) ...
    df_trend = load_station_trend_data(station['station_id'])
    if not df_trend.empty:
        st.line_chart(df_trend.set_index("Year")[["Max", "Average", "Min"]])

# 4. MAIN EXECUTION
df_station_data = load_station_data()

if not df_station_data.empty:
    if st.session_state["current_view"] == "map":
        view_map(df_station_data)
    elif st.session_state["current_view"] == "detail":
        view_detail()
else:
    st.error("❌ No station data found.")