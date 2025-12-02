# sanity.py
import os
import duckdb
import json
import logging

#logging.basicConfig(level=logging.INFO, format='[sanity.py] %(message)s')

DUCKDB_FILE = os.getenv("DUCKDB_FILE", "/data/gold/gold.duckdb")

def run_sanity_checks(duckdb_file=None):
    """Run sanity checks on the DuckDB file and return a dict report."""
    db_file = duckdb_file or DUCKDB_FILE
    con = duckdb.connect(db_file)

    tables = con.execute("SHOW TABLES").fetchall()
    row_count = con.execute("SELECT COUNT(*) FROM gold").fetchone()[0]
    sample_rows = con.execute("SELECT * FROM gold LIMIT 1").fetchdf().to_dict(orient="records")

    try:
        summary = con.execute("""
            SELECT crash_type, COUNT(*) AS cnt
            FROM gold
            GROUP BY crash_type
        """).fetchdf().to_dict(orient="records")
    except duckdb.Error:
        summary = []


    # Find duplicates
    result = con.execute("""
        SELECT crash_record_id, COUNT(*) AS cnt
        FROM gold
        GROUP BY crash_record_id
        HAVING COUNT(*) > 1
    """).fetchall()

    con.close()

    dupes = ""
    if result:
        dup_count = sum(row[1] for row in result)
        dup_ids = [row[0] for row in result]
        dupes = f"[Sanity] Found {len(result)} duplicate crash_record_id(s), total duplicate rows: {dup_count}"
        #logging.info(f"[Sanity] Duplicated IDs (sample up to 10): {dup_ids[:10]}")
    else:
        dupes = "[Sanity] No duplicate crash_record_id found in gold table."

    report = {
        "tables": tables,
        "row_count": row_count,
        "sample_rows": sample_rows,
        "dupes": dupes 
    }

    return report

def log_sanity_report(report):
    """Log the sanity report nicely via logging."""

    logging.info("[Sanity] Sanity report:")
    logging.info(json.dumps(report, indent=2))
