# duckdb_writer.py
import duckdb
import pandas as pd
import os
import logging

def write_to_duckdb(local_path: str, db_path: str = "/data/gold/gold.duckdb"):
    if not os.path.exists(local_path):
        raise FileNotFoundError(f"{local_path} missing")

    df = pd.read_csv(local_path)
    logging.info(f"Read {len(df)} rows, {len(df.columns)} cols from {local_path}")

    con = duckdb.connect(db_path)

    # create table if missing (schema from df)
    con.execute("CREATE TABLE IF NOT EXISTS gold AS SELECT * FROM df WHERE FALSE;")

    # ensure unique index (needed for some upsert forms)
    # create index only if table was just created would be ideal; wrap in try-except to be safe
    try:
        con.execute("CREATE UNIQUE INDEX IF NOT EXISTS gold_unique_crash ON gold(crash_record_id);")
    except Exception:
        # older duckdb may not support IF NOT EXISTS; ignore if fails
        pass
    
    key_col = "crash_record_id"


    all_cols = [c for c in df.columns]
    update_cols = [c for c in all_cols if c != key_col]

    # build "is distinct" condition
    distinct_conditions = " OR ".join(
        f"(target.{c} IS DISTINCT FROM source.{c})" for c in update_cols
    )

    set_clause = ", ".join(f"{c} = source.{c}" for c in update_cols)

    
    # Count total rows before
    rows_before = con.execute("SELECT COUNT(*) FROM gold").fetchone()[0]

    # Count rows that *will* be updated/inserted (pre-merge)
    rows_to_update = con.execute(f"""
        SELECT COUNT(*) FROM gold AS target
        JOIN df AS source USING ({key_col})
        WHERE {distinct_conditions};
    """).fetchone()[0]

    rows_to_insert = con.execute(f"""
        SELECT COUNT(*) FROM df AS source
        WHERE NOT EXISTS (
            SELECT 1 FROM gold AS target WHERE target.{key_col} = source.{key_col}
        );
    """).fetchone()[0]

    # Perform the merge (actual write)
    con.execute(f"""
        MERGE INTO gold AS target
        USING df AS source
        ON target.{key_col} = source.{key_col}
        WHEN MATCHED AND ({distinct_conditions}) THEN
            UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN
            INSERT *;
    """)

    # Count total rows after
    rows_after = con.execute("SELECT COUNT(*) FROM gold").fetchone()[0]

    # Print results
    logging.info("[duckdb_writer] Rows: Before / After / Updated / Inserted")
    logging.info(f"[duckdb_writer]      {rows_before} / {rows_after} / {rows_to_update} / {rows_to_insert}")
    #print(f"[duckdb_writer] Total rows before merge: {rows_before}")
    #print(f"[duckdb_writer] Updated {rows_to_update} existing rows")
    #print(f"[duckdb_writer] Inserted {rows_to_insert} new rows")
    #print(f"[duckdb_writer] Total rows after merge:  {rows_after}")
    #print(f"[duckdb_writer] Net row change: {rows_after - rows_before}")

    '''
    merge_sql = f"""
    MERGE INTO gold AS target
    USING df AS source
    ON target.{key_col} = source.{key_col}
    WHEN MATCHED AND ({distinct_conditions}) THEN
      UPDATE SET {set_clause}
    WHEN NOT MATCHED THEN
      INSERT *;
    """

    '''

    #con.execute(merge_sql)
    logging.info("MERGE upsert complete")
    con.close()