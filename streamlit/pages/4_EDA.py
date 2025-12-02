import streamlit as st
import matplotlib.pyplot as plt
from utils.eda_helpers import get_numeric_summary, get_categorical_summary, get_table_rowcount, get_crashes_by_hour, get_crashes_by_month

DB_PATH = "/data/gold/gold.duckdb"
TABLE = "gold"

st.title("🧮 Exploratory Data Overview")

# Define what’s numeric vs categorical
numeric_features = ["lighting_condition","veh_count","hour"]
categorical_features = ["road_defect","roadway_surface_cond","weather_condition"]

st.subheader("📊 Table Overview")
rowcount = get_table_rowcount(DB_PATH, TABLE)
st.write(f"Total rows in `{TABLE}`: **{rowcount:,}**")

st.divider()
st.subheader("🔢 Numeric Summary")
num_summary = get_numeric_summary(DB_PATH, TABLE, numeric_features)
st.dataframe(num_summary, use_container_width=True)

#st.divider()
st.subheader("🏷️ Categorical Summary")
cat_summary = get_categorical_summary(DB_PATH, TABLE, categorical_features)
st.dataframe(cat_summary, use_container_width=True)

st.subheader("🕒 Crashes by Hour of Day")

df_hourly = get_crashes_by_hour(DB_PATH, TABLE, "hour")


if df_hourly.empty:
    st.warning("No crash data available for plotting.")
else:
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(df_hourly["hour"], df_hourly["crash_count"], marker='o')
    ax.set_xlabel("Hour of Day (0–23)")
    ax.set_ylabel("Number of Crashes")
    ax.set_title("Crashes by Hour of Day")
    ax.grid(True)
    
    st.pyplot(fig)

st.subheader("📅 Crashes by Month")

df_monthly = get_crashes_by_month(DB_PATH, TABLE, "month")

if df_monthly.empty:
    st.warning("No crash data available for plotting.")
else:
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar(df_monthly["month"], df_monthly["crash_count"], color='skyblue')
    ax.set_xlabel("Month (1-12)")
    ax.set_ylabel("Number of Crashes")
    ax.set_title("Crashes by Month")
    ax.set_xticks(range(1, 13))  # ensure all months 1-12 are shown
    ax.grid(axis='y', linestyle='--', alpha=0.7)

    st.pyplot(fig)
