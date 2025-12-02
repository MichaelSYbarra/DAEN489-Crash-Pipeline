import streamlit as st
import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import ast

sns.set_style("whitegrid")
st.set_page_config(page_title="Crash EDA Dashboard", layout="wide")
st.title("🚦 Crash Data EDA Dashboard")

# --- Load gold.duckdb ---
db_path = "/data/gold/gold.duckdb"

if not os.path.exists(db_path):
    st.error("❌ gold.duckdb not found at /data/gold/gold.duckdb")
    st.stop()

con = duckdb.connect(db_path)
df = con.execute("SELECT * FROM gold").fetchdf()
con.close()

# --- Basic cleaning ---
if "crash_date" in df.columns:
    df["crash_date"] = pd.to_datetime(df["crash_date"], errors="coerce")

# --- Create Tabs ---
tab1, tab2, tab3, tab4 = st.tabs([
    "🕒 Time", "🌤️ Conditions", "🚗 Vehicles", "👥 People"
])

# =====================================================
# 🕒 TAB 1: TIME TRENDS
# =====================================================
with tab1:
    st.header("🕒 Temporal Trends")

    # 1. Crashes Over Time
    st.subheader("Crashes per Month with Rolling Average")

    # Ensure crash_date is datetime
    df["crash_date"] = pd.to_datetime(df["crash_date"], errors="coerce")

    # Aggregate crashes per month (ignoring year)
    df["month_only"] = df["crash_date"].dt.month
    monthly_counts = df.groupby("month_only").size().sort_index()

    # Smooth with rolling average (e.g., 3-month window)
    rolling_avg = monthly_counts.rolling(window=3, center=True).mean()

    # Plot
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(monthly_counts.index, monthly_counts.values, marker='o', label="Monthly Crashes")
    ax.plot(rolling_avg.index, rolling_avg.values, color='red', linestyle='--', label="3-Month Rolling Avg")
    ax.set_xticks(range(1, 13))
    ax.set_xticklabels(["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"])
    ax.set_xlabel("Month")
    ax.set_ylabel("Number of Crashes")
    ax.set_title("Crashes per Month (Smoothed)")
    ax.legend()
    st.pyplot(fig)

    # 2. Crashes by Hour
    st.subheader("Crashes by Hour")
    fig, ax = plt.subplots(figsize=(10, 4))
    sns.countplot(x="hour", hue="is_weekend", data=df, ax=ax)
    ax.set_title("Crashes by Hour (Weekend vs Weekday)")
    st.pyplot(fig)

    # 3. Crashes by Month-Year Heatmap
    st.subheader("Crash Severity by Season (Pie Chart)")

    # --- Map months to seasons ---
    def month_to_season(month):
        if month in [12, 1, 2]:
            return "Winter"
        elif month in [3, 4, 5]:
            return "Spring"
        elif month in [6, 7, 8]:
            return "Summer"
        else:
            return "Fall"

    df["season"] = df["crash_date"].dt.month.apply(month_to_season)

    # --- Calculate severity ratio per season ---
    severity_ratio = df.groupby("season")["crash_type_binary"].mean()

    # Ensure all seasons are present (fill missing with 0)
    all_seasons = ["Winter", "Spring", "Summer", "Fall"]
    severity_ratio = severity_ratio.reindex(all_seasons, fill_value=0)

    # --- Plot Pie Chart ---
    fig, ax = plt.subplots(figsize=(6,6))

    # Handle the case where all values are 0 (no crashes)
    if severity_ratio.sum() == 0:
        ax.text(0.5, 0.5, "No data available", ha="center", va="center", fontsize=14)
        ax.axis("off")
    else:
        ax.pie(severity_ratio, labels=severity_ratio.index, autopct='%1.1f%%',
            startangle=90, colors=sns.color_palette("Set2"))

    ax.set_title("Proportion of Severe Crashes by Season")
    st.pyplot(fig)
# =====================================================
# 🌤️ TAB 2: ENVIRONMENTAL & ROAD CONDITIONS
# =====================================================
with tab2:
    st.header("🌤️ Environmental & Road Conditions")

    # 4. Crashes by Weather
    st.subheader("Crashes by Weather Condition")
    fig, ax = plt.subplots(figsize=(10, 4))
    sns.countplot(y="weather_condition", data=df, order=df["weather_condition"].value_counts().index, ax=ax)
    ax.set_title("Crashes by Weather Condition")
    st.pyplot(fig)
    st.markdown("Most Crashes are during Clear weather, but perhaps there is a correlation between weather and the severity?")

    # 5. Lighting Condition
    st.subheader("Lighting Conditions")
    fig, ax = plt.subplots(figsize=(10, 4))
    sns.countplot(y="lighting_condition", data=df, order=df["lighting_condition"].value_counts().index, ax=ax)
    ax.set_title("Crashes by Lighting Condition")
    st.pyplot(fig)
    st.markdown("Most crashes are happening during the day but the middling light (dawn/dusk) is second suggesting this time could be dangerous?")

    # 6. Road Surface vs Defects
    st.subheader("Road Surface vs Defects")
    agg = df.groupby(["roadway_surface_cond", "road_defect"]).size().reset_index(name="count")
    fig, ax = plt.subplots(figsize=(10, 4))
    sns.barplot(x="roadway_surface_cond", y="count", hue="road_defect", data=agg, ax=ax)
    ax.set_title("Crashes by Surface Condition and Road Defect")
    st.pyplot(fig)
    "Even sorting by these different types of roads, defects are actually very few or under-reported as marked via unknown. This doesn't communicate that much information as high use roads where most crashes are, are expected to be maintained."

# =====================================================
# 🚗 TAB 3: VEHICLES
# =====================================================
with tab3:
    st.header("🚗 Vehicle Insights")

    # 7. Vehicle Counts per Crash
    st.subheader("Vehicle Counts per Crash")
    fig, ax = plt.subplots(figsize=(8, 4))
    sns.histplot(df["veh_count"], bins=range(int(df["veh_count"].max()) + 2), kde=False, ax=ax)
    ax.set_xlabel("Number of Vehicles")
    ax.set_ylabel("Frequency")
    st.pyplot(fig)
    st.markdown("Most crashes are two vehicle crashes. Crashing into stationary object (one vehicle) crashes and large pile ups are a lot less common.")


    TOP_N = 15  # only show top N categories

    # --- VEHICLE MANEUVERS ---
    st.subheader("Vehicle Maneuvers (Top 15)")

    # Parse JSON strings into Python lists
    def parse_json_list(x):
        try:
            if pd.isna(x):
                return []
            if isinstance(x, str):
                return ast.literal_eval(x)  # converts '["turning","stopped"]' -> list
            if isinstance(x, list):
                return x
            return []
        except:
            return []

    veh_maneuvers_flat = df["veh_maneuver_list_json"].apply(parse_json_list).explode()
    maneuver_counts = veh_maneuvers_flat.value_counts().head(TOP_N)

    fig, ax = plt.subplots(figsize=(10, 5))
    sns.barplot(x=maneuver_counts.values, y=maneuver_counts.index, ax=ax, palette="viridis")
    ax.set_xlabel("Number of Crashes")
    ax.set_ylabel("Maneuver")
    ax.set_title(f"Top {TOP_N} Vehicle Maneuvers Involved in Crashes")
    st.pyplot(fig)
    st.markdown("Most crashes are caused due to one participant smacking into another straight ahead. Further analysis could suggest certain roads are dangerous with many cars turning in getting smacked straight ahead or similar insights.")

    # --- VEHICLE DEFECTS ---
    st.subheader("Vehicle Defects (Top 15)")

    veh_defects_flat = df["veh_vehicle_defect_list_json"].apply(parse_json_list).explode()
    defect_counts = veh_defects_flat.value_counts().head(TOP_N)

    fig, ax = plt.subplots(figsize=(10, 5))
    sns.barplot(x=defect_counts.values, y=defect_counts.index, ax=ax, palette="magma")
    ax.set_xlabel("Number of Crashes")
    ax.set_ylabel("Defect")
    ax.set_title(f"Top {TOP_N} Vehicle Defects Involved in Crashes")
    st.pyplot(fig)
    st.markdown("This graph tells us that it is not likely that vehicle errors are the main causes of crashes here.")

    # 10. Vehicle Use Types
    st.subheader("Vehicle Use Types")
    use_counts = df[["has_emergency", "has_commercial", "has_personal", "has_bicycle"]].sum()
    fig, ax = plt.subplots(figsize=(8, 4))
    use_counts.plot(kind="bar", ax=ax)
    ax.set_ylabel("Number of Crashes")
    st.pyplot(fig)
    st.markdown("The majority of crashes involve mostly personal use vehicles. Further analysis could look into whether different types of vehicles involved imfluence crash severity.")

# =====================================================
# 👥 TAB 4: PEOPLE & DEMOGRAPHICS
# =====================================================
with tab4:
    st.header("👥 People Involved")

    # 11. Average Age by Hour
    st.subheader("Average Age by Hour")
    avg_age = df.groupby("hour")["age_mean"].mean().reset_index()
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(avg_age["hour"], avg_age["age_mean"])
    ax.set_xlabel("Hour")
    ax.set_ylabel("Average Age")
    st.pyplot(fig)
    st.markdown("We can see here the times when different age demographics are on the road.")

    # 12. Age Range Distribution
    st.subheader("Age Distribution")
    fig, ax = plt.subplots(figsize=(8, 4))
    sns.boxplot(data=df[["age_min", "age_mean", "age_max"]], ax=ax)
    ax.set_title("Age Min/Mean/Max Distribution")
    st.pyplot(fig)
    st.markdown("This is a bit more detailed than this really needs to be, but we can see the averages for the youngest person involved vs the oldest in the crash. Further analysis should bucket these and compare total amounts.")

    # 13. People Count per Crash
    st.subheader("People Count per Crash")
    fig, ax = plt.subplots(figsize=(8, 4))
    sns.histplot(df["ppl_count"], bins=range(int(df["ppl_count"].max()) + 2), kde=False, ax=ax)
    ax.set_xlabel("Number of People")
    ax.set_ylabel("Frequency")
    st.pyplot(fig)
    st.markdown("Most vehicles on the road are carrying a single person, the driver. Perhaps this is contributing to high traffic volume and thus more crashes?")

    # 14. Weekend vs Weekday Crashes
    st.subheader("Weekend vs Weekday Crashes (Normalized per Day)")

    # Count crashes per is_weekend
    crash_counts = df.groupby("is_weekend").size()

    # Normalize by number of days
    # Assume is_weekend == False -> weekday, True -> weekend
    normalized_counts = crash_counts.copy()
    normalized_counts[False] = crash_counts.get(False, 0) / 5  # weekday average per day
    normalized_counts[True] = crash_counts.get(True, 0) / 2   # weekend average per day

    # Plot
    fig, ax = plt.subplots(figsize=(6, 4))
    sns.barplot(x=normalized_counts.index.map({False: "Weekday", True: "Weekend"}),
                y=normalized_counts.values, ax=ax, palette="Set2")
    ax.set_ylabel("Total Crashes")
    ax.set_title("Normalized Weekend vs Weekday Crash Counts")
    st.pyplot(fig)
    st.markdown("Weekends cause more crashes even after normalized for how mahy days they have in a week compared to two weekend days. Perhaps rush hour traffic is causing this?")

    # 15. Crash Type Binary by Hour Bin
    st.subheader("Crash Type by Hour Bin")
    pivot = df.pivot_table(index="hour_bin", columns="crash_type_binary", aggfunc="size", fill_value=0)
    fig, ax = plt.subplots(figsize=(10, 4))
    pivot.plot(kind="bar", stacked=True, ax=ax)
    ax.set_ylabel("Number of Crashes")
    ax.set_xlabel("Hour Bin")
    st.pyplot(fig)
    st.markdown("This graph can show us which time periods are most dangerous. It seems 12-17 has more severe crashes.")