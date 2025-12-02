import streamlit as st
import pandas as pd
from datetime import datetime
import os

CRON_FILE = "/data/cron_jobs.csv"  # make sure this folder is volume-mounted for persistence

st.title("⏰ Scheduler")

# --- Scheduler Form ---
st.header("Create a new scheduled job")

freq = st.selectbox("Frequency", ["daily", "weekly"])

start_time = st.time_input("Start time (HH:MM)", value=datetime.now().time())

job_name = st.text_input("Job Name", value=f"job_{datetime.now().strftime('%Y%m%d%H%M%S')}")

if st.button("Generate Cron & Save"):
    # Build cron expression
    minute = start_time.minute
    hour = start_time.hour
    if freq == "daily":
        cron_expr = f"{minute} {hour} * * *"
    else:  # weekly
        day_of_week = st.selectbox("Day of Week", list(range(0, 7)), index=datetime.today().weekday())
        cron_expr = f"{minute} {hour} * * {day_of_week}"

    st.success(f"Cron expression: `{cron_expr}`")

    # Save to CSV
    new_row = {
        "job_name": job_name,
        "frequency": freq,
        "start_time": start_time.strftime("%H:%M"),
        "cron": cron_expr,
        "created_at": datetime.now().isoformat()
    }

    if os.path.exists(CRON_FILE):
        df_cron = pd.read_csv(CRON_FILE)
        df_cron = pd.concat([df_cron, pd.DataFrame([new_row])], ignore_index=True)
    else:
        df_cron = pd.DataFrame([new_row])

    df_cron.to_csv(CRON_FILE, index=False)
    st.success(f"Job saved successfully! Total jobs: {len(df_cron)}")

# --- Existing Jobs ---
st.header("Existing Scheduled Jobs")
if os.path.exists(CRON_FILE):
    df_jobs = pd.read_csv(CRON_FILE)
    st.dataframe(df_jobs)
else:
    st.info("No cron jobs scheduled yet.")