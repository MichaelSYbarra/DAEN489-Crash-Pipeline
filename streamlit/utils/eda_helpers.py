import duckdb
import pandas as pd

def get_numeric_summary(db_path: str, table: str, numeric_cols: list[str]) -> pd.DataFrame:
    """Compute count, min, mean, max for numeric columns."""
    con = duckdb.connect(db_path)
    results = []

    for col in numeric_cols:
        query = f"""
        SELECT 
            
            MIN({col}) AS min,
            AVG({col}) AS mean,
            MAX({col}) AS max
        FROM {table}
        """
        row = con.execute(query).fetchone()
        results.append({
            "column": col,
            
            "min": row[0],
            "mean": row[1],
            "max": row[2],
        })

    con.close()
    return pd.DataFrame(results)


def get_categorical_summary(db_path: str, table: str, cat_cols: list[str]) -> pd.DataFrame:
    """Compute top 2 categories and their counts for each categorical column."""
    con = duckdb.connect(db_path)
    results = []

    for col in cat_cols:
        query = f"""
        SELECT {col}, COUNT(*) AS freq
        FROM {table}
        GROUP BY {col}
        ORDER BY freq DESC
        LIMIT 2
        """
        df_top = con.execute(query).df()

        if not df_top.empty:
            top_values = [f"{r[0]} ({r[1]})" for r in df_top.itertuples(index=False)]
            summary = " | ".join(top_values)
        else:
            summary = "N/A"

        results.append({
            "column": col,
            "top two categories": summary
        })

    con.close()
    return pd.DataFrame(results)


def get_table_rowcount(db_path: str, table: str) -> int:
    """Return total number of rows in the table."""
    con = duckdb.connect(db_path)
    count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    con.close()
    return count

def get_crashes_by_hour(db_path: str, table: str, hour_col: str = "hour") -> pd.DataFrame:
    """Return a DataFrame with crash counts grouped by hour."""
    con = duckdb.connect(db_path)
    query = f"""
        SELECT {hour_col} AS hour, COUNT(*) AS crash_count
        FROM {table}
        GROUP BY {hour_col}
        ORDER BY {hour_col}
    """
    df = con.execute(query).df()
    con.close()
    return df

def get_crashes_by_month(db_path: str, table: str, month_col: str = "month") -> pd.DataFrame:
    """Return a DataFrame with crash counts grouped by month."""
    con = duckdb.connect(db_path)
    query = f"""
        SELECT {month_col} AS month, COUNT(*) AS crash_count
        FROM {table}
        GROUP BY {month_col}
        ORDER BY {month_col}
    """
    df = con.execute(query).df()
    con.close()
    return df