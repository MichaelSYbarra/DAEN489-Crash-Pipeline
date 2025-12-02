# metrics.py
from prometheus_client import Counter, Gauge, Histogram

# Count total messages processed, labeled by outcome
CLEAN_MESSAGES_TOTAL = Counter(
    "clean_messages_total",
    "Total clean messages processed",
    ["result"]  # processed, ignored, failed
)

# Service uptime
CLEANER_UPTIME_SECONDS = Gauge(
    "cleaner_uptime_seconds",
    "Service uptime in seconds"
)

RABBIT_CONNECTIONS = Gauge(
    "cleaner_rabbitmq_connections",
    "Active RabbitMQ connections"
)

MINIO_ROWS_READ_TOTAL = Counter(
    "minio_rows_read_total",
    "Total number of rows read from MinIO files",
    ["bucket"]
)

CLEANER_RUN_DURATION_SECONDS = Histogram(
    "cleaner_run_duration_seconds",
    "Time it takes to run a full cleaning cycle",
    ["bucket"],  
    buckets=[0.5, 1, 2, 5, 10, 30, 60, 120, float("inf")]  
)
