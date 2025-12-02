import streamlit as st
import datetime

import streamlit as st
from prometheus_client import make_wsgi_app, Gauge, Summary
from werkzeug.middleware.dispatcher import DispatcherMiddleware
from streamlit.web.server.server import Server

# --- PATCH STREAMLIT WITH PROMETHEUS ---
prom_app = make_wsgi_app()
st_server = Server.get_current()
st_server._wsgi_app = DispatcherMiddleware(st_server._wsgi_app, {"/metrics": prom_app})

# --- DEFINE METRICS ---
# Initialize Prometheus metrics in session_state if not already
if "UPTIME_GAUGE" not in st.session_state:
    st.session_state["UPTIME_GAUGE"] = Gauge(
        "app_uptime_seconds", "Total uptime of the Streamlit app"
    )

if "ACCURACY_GAUGE" not in st.session_state:
    st.session_state["ACCURACY_GAUGE"] = Gauge(
        "model_accuracy", "Model accuracy on last run"
    )

if "PRECISION_GAUGE" not in st.session_state:
    st.session_state["PRECISION_GAUGE"] = Gauge(
        "model_precision", "Model precision on last run"
    )

if "RECALL_GAUGE" not in st.session_state:
    st.session_state["RECALL_GAUGE"] = Gauge(
        "model_recall", "Model recall on last run"
    )

if "LATENCY_SUMMARY" not in st.session_state:
    st.session_state["LATENCY_SUMMARY"] = Summary(
        "prediction_latency_seconds", "Prediction latency per batch"
    )




st.set_page_config(
    page_title="Crash Prediction Dashboard",
    page_icon="🚦",
    layout="wide",
)

# -------------------------------------------------------------------
# MAIN HEADER
# -------------------------------------------------------------------
st.title("🚦 Crash Prediction Dashboard")
st.write(
    """
    Welcome to the **Crash Type Prediction Dashboard**, the control center for your end-to-end 
    machine learning pipeline.  
    Use the sidebar to explore data, monitor container health, and trigger new cleaning runs.
    """
)

# -------------------------------------------------------------------
# PIPELINE SUMMARY
# -------------------------------------------------------------------
st.subheader("🧠 Pipeline Summary")

st.markdown("""
- **Goal:** Predict crash severity (`crash_type_binary`)  
- **Type:** Binary classification — *Property Damage Only* vs *Injury/Fatal*  
- **Model Inputs:** Weather, lighting, road surface, speed limit, and crash time  
- **Gold Table:** `gold` (stored in DuckDB)  
- **Pipeline Stages:** Extract → Transform → Clean → Store → Predict
""")

# -------------------------------------------------------------------
# STATUS AT A GLANCE
# -------------------------------------------------------------------
st.divider()
st.subheader("📦 Active Containers")

st.markdown("""
- 🟢 **RabbitMQ** — message broker for pipeline coordination  
- 🟢 **MinIO** — object storage for raw and transformed data  
- 🟢 **Extractor** — pulls and uploads data  
- 🟢 **Transformer** — prepares data for model input  
- 🟢 **Cleaner** — applies business rules and upserts into DuckDB  
- 🟢 **Streamlit** — this dashboard
""")

st.info("See **Home** page for detailed pipeline info and real-time health checks.")

# -------------------------------------------------------------------
# FOOTER
# -------------------------------------------------------------------
st.divider()
st.caption(f"Dashboard last loaded: {datetime.datetime.now():%Y-%m-%d %H:%M:%S}")
st.caption("Crash Prediction System © 2025 • Built with Streamlit + DuckDB + MinIO + RabbitMQ")
