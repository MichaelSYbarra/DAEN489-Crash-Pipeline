import streamlit as st
from minio import Minio
import json
import os
from datetime import datetime
from io import BytesIO
import duckdb

# --------------------------
# 🔐 MinIO Connection Setup
# --------------------------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_USER", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_PASS", "minio123")

client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

BUCKET = "raw-data"
PREFIX = "_runs/"

# --------------------------
# 📦 Helper Functions
# --------------------------

def get_latest_manifest(bucket: str, prefix: str = "_runs/"):
    """Get the latest manifest.json from MinIO."""
    try:
        objects = list(client.list_objects(bucket, prefix=prefix, recursive=True))
        manifests = [o for o in objects if o.object_name.endswith("manifest.json")]

        if not manifests:
            return None, None

        latest = sorted(manifests, key=lambda x: x.last_modified, reverse=True)[0]
        data = client.get_object(bucket, latest.object_name)
        manifest = json.load(BytesIO(data.read()))
        data.close()
        data.release_conn()

        return manifest, latest.object_name
    except Exception as e:
        st.error(f"Error fetching manifest: {e}")
        return None, None


def get_total_runs(bucket: str, prefix: str = "_runs/"):
    """Count total unique run folders in MinIO."""
    try:
        objects = list(client.list_objects(bucket, prefix=prefix, recursive=False))
        folders = set(
            obj.object_name.split("/")[1]
            for obj in objects
            if "/" in obj.object_name
        )
        return len(folders)
    except Exception as e:
        st.error(f"Error counting runs: {e}")
        return None


def get_gold_metrics(db_path: str = "/data/gold/gold.duckdb"):
    """Fetch summary statistics from the gold.duckdb table."""
    if not os.path.exists(db_path):
        return {
            "exists": False,
            "row_count": None,
            "latest_date": None,
            "last_modified": None
        }

    try:
        con = duckdb.connect(db_path)
        row_count = con.execute("SELECT COUNT(*) FROM gold").fetchone()[0]
        latest_date = con.execute("SELECT MAX(crash_date) FROM gold").fetchone()[0]
        con.close()

        last_modified = datetime.fromtimestamp(os.path.getmtime(db_path)).astimezone()

        return {
            "exists": True,
            "row_count": row_count,
            "latest_date": latest_date,
            "last_modified": last_modified
        }

    except Exception as e:
        st.error(f"Error reading gold table: {e}")
        return {
            "exists": True,
            "row_count": None,
            "latest_date": None,
            "last_modified": None
        }

# --------------------------
# 🧭 Pipeline Report Layout
# --------------------------

st.header("🧭 Pipeline Report")

# Fetch data
manifest, manifest_path = get_latest_manifest(BUCKET)
total_runs = get_total_runs(BUCKET)
gold_stats = get_gold_metrics()

# --------------------------
# 📊 Summary Cards (at a glance)
# --------------------------

st.subheader("📈 Summary Overview")

col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Total Runs Completed", total_runs if total_runs is not None else "N/A")

with col2:
    st.metric("Latest corrid", manifest.get("corr", "N/A") if manifest else "N/A")
    if manifest:
        st.caption("Click to copy:")
        st.code(manifest.get("corr", ""), language=None)

with col3:
    if gold_stats["exists"]:
        st.metric("Gold Row Count", f"{gold_stats['row_count']:,}" if gold_stats["row_count"] else "N/A")

col4, col5 = st.columns(2)
with col4:
    if gold_stats["exists"]:
        st.metric("Latest Data Date (Gold)", gold_stats["latest_date"] or "N/A")

with col5:
    if manifest and manifest.get("finished_at"):
        end_local = datetime.fromisoformat(
            manifest["finished_at"].replace("Z", "+00:00")
        ).astimezone()
        st.metric("Last Run Ended At", end_local.strftime("%Y-%m-%d %H:%M:%S"))

st.markdown("---")

# --------------------------
# 🧾 Latest Run Summary
# --------------------------

with st.expander("🧾 Latest Run Summary", expanded=True):
    if not manifest:
        st.warning("No manifests found in MinIO yet.")
    else:
        mode = manifest.get("mode", "N/A")
        start = manifest.get("started_at", "")
        end = manifest.get("finished_at", "")

        if start and end:
            start_local = datetime.fromisoformat(start.replace("Z", "+00:00")).astimezone()
            end_local = datetime.fromisoformat(end.replace("Z", "+00:00")).astimezone()
            duration = (end_local - start_local).total_seconds()
        else:
            start_local = end_local = duration = "N/A"

        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Mode", mode)
        with col2:
            st.metric("Duration (s)", f"{duration:.2f}" if isinstance(duration, float) else "N/A")
        with col3:
            st.metric("Started / Ended", f"{start_local}\n→ {end_local}" if start_local != "N/A" else "N/A")

        #st.markdown("---")
        #st.subheader("📋 Config Used")
        #st.json(manifest.get("config", {}))

        st.markdown("---")
        st.subheader("📊 Run Details (Raw Manifest)")
        st.json(manifest)

# --------------------------
# 🏆 Gold Table Health
# --------------------------

with st.expander("🏆 Gold Table Health", expanded=False):
    st.subheader("🏆 Gold Table Summary")

    if not gold_stats["exists"]:
        st.warning("No gold.duckdb found at /data/gold/gold.duckdb")
    else:
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Row Count", f"{gold_stats['row_count']:,}" if gold_stats["row_count"] else "N/A")
        with col2:
            st.metric("Latest Crash Date", gold_stats["latest_date"] or "N/A")
        with col3:
            st.metric("Last Modified", gold_stats["last_modified"].strftime("%Y-%m-%d %H:%M:%S"))

with st.expander("Table Schema Viewer", expanded = False):
    st.subheader("📋 Gold Table Schema Viewer")

    db_path = "/data/gold/gold.duckdb"

    if not os.path.exists(db_path):
        st.error("⚠️ gold.duckdb not found at /data/gold/gold.duckdb")
    else:
        try:
            con = duckdb.connect(db_path)

            # Get list of available tables
            tables = [t[0] for t in con.execute("SHOW TABLES").fetchall()]

            if not tables:
                st.warning("No tables found in the gold database.")
            else:
                selected_table = st.selectbox("Select a table to inspect:", tables)

                if selected_table:
                    st.markdown(f"### 🧱 Schema for `{selected_table}`")

                    # Fetch schema info
                    schema_df = con.execute(f"PRAGMA table_info('{selected_table}')").fetchdf()
                    st.dataframe(schema_df, use_container_width=True)

                    # Optionally, show column names as a list
                    cols = schema_df["name"].tolist()
                    with st.expander("🧾 Column Headers (copyable)"):
                        st.code(", ".join(cols), language=None)

            con.close()

        except Exception as e:
            st.error(f"Error reading DuckDB: {e}")

import pandas as pd
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet
from io import BytesIO

# --------------------------
# 📤 Download Reports Section
# --------------------------

st.markdown("---")
st.subheader("📤 Download Reports")

# --- Prepare data for export ---
if manifest:
    start = manifest.get("started_at", "")
    end = manifest.get("finished_at", "")
    if start and end:
        start_local = datetime.fromisoformat(start.replace("Z", "+00:00")).astimezone()
        end_local = datetime.fromisoformat(end.replace("Z", "+00:00")).astimezone()
        duration = (end_local - start_local).total_seconds()
    else:
        duration = None

    data = {
        "total_runs": [total_runs],
        "corr": [manifest.get("corr", "N/A")],
        "mode": [manifest.get("mode", "N/A")],
        "started_at": [manifest.get("started_at", "N/A")],
        "finished_at": [manifest.get("finished_at", "N/A")],
        "duration_seconds": [duration],
        "gold_row_count": [gold_stats.get("row_count") if gold_stats else "N/A"],
        "gold_latest_date": [gold_stats.get("latest_date") if gold_stats else "N/A"],
        "gold_last_modified": [gold_stats.get("last_modified").strftime("%Y-%m-%d %H:%M:%S") if gold_stats and gold_stats.get("last_modified") else "N/A"]
    }

    df = pd.DataFrame(data)

    # --- CSV Export ---
    csv_buffer = BytesIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    # --- PDF Export ---
    pdf_buffer = BytesIO()
    doc = SimpleDocTemplate(pdf_buffer, pagesize=letter)
    styles = getSampleStyleSheet()
    story = []

    story.append(Paragraph("Pipeline Summary Report", styles["Title"]))
    story.append(Spacer(1, 12))

    summary_data = [
        ["Total Runs Completed", total_runs],
        ["Latest corrid", manifest.get("corr", "N/A")],
        ["Mode", manifest.get("mode", "N/A")],
        ["Started At", manifest.get("started_at", "N/A")],
        ["Finished At", manifest.get("finished_at", "N/A")],
        ["Duration (seconds)", f"{duration:.2f}" if duration else "N/A"],
        ["Gold Row Count", f"{gold_stats.get('row_count'):,}" if gold_stats.get("row_count") else "N/A"],
        ["Latest Data Date", str(gold_stats.get("latest_date")) if gold_stats.get("latest_date") else "N/A"],
        ["Gold Last Modified", gold_stats.get("last_modified").strftime("%Y-%m-%d %H:%M:%S") if gold_stats.get("last_modified") else "N/A"],
    ]

    table = Table(summary_data, colWidths=[200, 250])
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.lightgrey),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 10),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
    ]))

    story.append(table)
    doc.build(story)
    pdf_buffer.seek(0)

    # --- Buttons ---
    col1, col2 = st.columns(2)
    with col1:
        st.download_button(
            "📄 Download CSV Report",
            data=csv_buffer,
            file_name="pipeline_summary.csv",
            mime="text/csv"
        )
    with col2:
        st.download_button(
            "📘 Download PDF Report",
            data=pdf_buffer,
            file_name="pipeline_summary.pdf",
            mime="application/pdf"
        )

else:
    st.warning("No latest run found — nothing to export yet.")
