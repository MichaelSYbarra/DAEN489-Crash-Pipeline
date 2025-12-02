import duckdb
import os
import pandas as pd

GOLD_PATH = os.getenv("GOLD_PATH", "/data/gold/gold.duckdb")

def get_gold_status():
    if not os.path.exists(GOLD_PATH):
        return {"exists": False, "tables": {}, "rows": 0}
    con = duckdb.connect(GOLD_PATH, read_only=True)
    tables = con.execute("SHOW TABLES").fetchall()
    counts = {}
    total = 0
    for (t,) in tables:
        n = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        counts[t] = n
        total += n
    con.close()
    return {"exists": True, "tables": counts, "rows": total}

def wipe_gold():
    if os.path.exists(GOLD_PATH):
        os.remove(GOLD_PATH)
        return True
    return False

def sample_gold(columns=None, limit=50):
    if not os.path.exists(GOLD_PATH):
        return pd.DataFrame()
    con = duckdb.connect(GOLD_PATH, read_only=True)
    table = con.execute("SHOW TABLES").fetchone()
    if not table:
        return pd.DataFrame()
    t = table[0]
    cols = "*" if not columns else ", ".join(columns)
    df = con.execute(f"SELECT {cols} FROM {t} LIMIT {limit}").df()
    con.close()
    return df