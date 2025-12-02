from prometheus_client import Gauge, start_http_server
import duckdb
import os
import time

# --- CONFIG ---
DUCKDB_FILE = "/data/gold/gold.duckdb"   # Update path if needed
TABLE_NAME = "gold"                       # Change if your gold table has a different name
SCRAPE_INTERVAL = 10                      # seconds

# --- PROMETHEUS METRICS ---
DUCKDB_FILE_SIZE_BYTES = Gauge(
    "duckdb_file_size_bytes",
    "Size of the DuckDB file in bytes"
)

DUCKDB_GOLD_TABLE_ROWS = Gauge(
    "duckdb_gold_table_rows",
    "Row count of the DuckDB gold table"
)

def update_metrics():
    # 1. File size
    try:
        size = os.path.getsize(DUCKDB_FILE)
        DUCKDB_FILE_SIZE_BYTES.set(size)
    except FileNotFoundError:
        DUCKDB_FILE_SIZE_BYTES.set(0)

    # 2. Row count of gold table
    try:
        con = duckdb.connect(DUCKDB_FILE, read_only=True)
        result = con.execute(f"SELECT COUNT(*) FROM {TABLE_NAME};").fetchone()
        DUCKDB_GOLD_TABLE_ROWS.set(result[0])
        con.close()
    except Exception:
        DUCKDB_GOLD_TABLE_ROWS.set(0)

def main():
    print("Starting DuckDB Exporter on port 9104...")
    start_http_server(9104)

    while True:
        update_metrics()
        time.sleep(SCRAPE_INTERVAL)

if __name__ == "__main__":
    main()
