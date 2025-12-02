# metrics.py
from prometheus_client import Counter, Gauge, Histogram


# Service uptime
STREAMLIT_UPTIME_SECONDS = Gauge(
    "streamlit_uptime_seconds",
    "Service uptime in seconds"
)

