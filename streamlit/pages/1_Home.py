import streamlit as st
import requests
from utils.health import check_service_health


st.set_page_config(page_title="🏠 Crash Type Prediction Dashboard", layout="wide")

# -------------------------------------------------------------------
# HEADER
# -------------------------------------------------------------------
st.title("🏠 Crash Type Prediction Dashboard")
st.write(
    """
    This dashboard provides an overview of the **Crash Type Prediction Pipeline** 
    and live health status of all running containers in the system.
    """
)

# -------------------------------------------------------------------
# SECTION A: PIPELINE OVERVIEW
# -------------------------------------------------------------------
st.subheader("🚗 Crash Type Prediction Pipeline Overview")

with st.container(border=True):
    st.markdown("### 🚗 Crash Type Classifier")
    st.markdown(
        "**Label predicted:** `crash_type_binary` • Type: `binary` • Classes: `No-Injury / Driveaway`, `Injury/Fatal`"
    )

    st.markdown(
        "I built a model to predict **crash severity** (injury/fatal vs no injury) "
        "using crash context such as weather, lighting, and road conditions."
    )

    st.markdown("**Key features:**")
    st.markdown("""
    - `weather_condition` — environmental visibility and surface risk
    - `lighting_condition` — influences driver response time
    - `road_surface_condition` — affects traction and stopping distance
    - `posted_speed_limit` — proxy for energy of impact
    - `crash_hour` — captures high-risk temporal patterns
    """)

    st.markdown("**Source columns (subset):**")
    st.markdown("""
    - `crashes`: crash_record_id, crash_date, weather_condition, lighting_condition, posted_speed_limit  
    """)

    st.markdown("**Class imbalance:**")
    st.markdown("""
    - Positives (Injury/Fatal): ~22%  
    - Negatives (PDO): ~78%  
    - Handling: `class_weight='balanced'`
    """)

    st.markdown("**Data grain & filters:**")
    st.markdown("""
    - One row = crash  
    - Window: 2020–present  
    - Filters: removed nulls, cleaned and consolidated messy and high cardinality categorical collumns
    """)

    st.markdown("**Leakage & caveats:**")
    st.markdown("""
    - Post-outcome variables (citations, EMS response time) excluded  
    - Crash_record_id only used as unique key  
    """)

    st.markdown("**Gold Table:** `gold`")

# -------------------------------------------------------------------
# SECTION B: CONTAINER HEALTH
# -------------------------------------------------------------------
st.divider()
st.subheader("💽 Container Health")

health = check_service_health()

cols = st.columns(3)
for i, (service, status) in enumerate(health.items()):
    #color = "🟢" if status in ("healthy", "running") else "🔴"
    cols[i % 3].markdown(
        f"**{service.capitalize()}**<br>Status: `{status}`",
        unsafe_allow_html=True,
    )