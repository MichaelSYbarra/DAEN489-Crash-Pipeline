# metrics.py 
# The transformer version
from prometheus_client import Counter, Gauge, Histogram

TRANSFORMER_MESSAGES_TOTAL = Counter(
    "transformer_messages_total",
    "Total transformer messages processed",
    ["result"]  # processed, ignored, failed
)

# Service uptime
TRANSFORMER_UPTIME_SECONDS = Gauge(
    "transformer_uptime_seconds",
    "Service uptime in seconds"
)

TRANSFORMER_ROWS_PROCESSED_TOTAL = Counter(
    "transformer_rows_processed_total",
    "Total number of rows in the resulting merged.csv",
)

TRANSFORM_RUN_DURATION_SECONDS = Histogram(
    "transform_run_duration_seconds",
    "Time it takes to run a full transform cycle",
    buckets=[0.5, 1, 2, 5, 10, 30, 60, 120, float("inf")]  
)

TRANSFORM_JOBS_TOTAL = Counter(
"transform_jobs_total",
"Total number of transform jobs processed",
["status"] # "success" or "failure"
)