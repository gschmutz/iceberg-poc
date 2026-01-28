import sys
import os
import json
import statistics
import ast
import pandas as pd
import numpy as np
import trino
from trino.auth import BasicAuthentication
from scipy.stats import zscore

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../lib')))
from util import get_param, get_credential

TRINO_USER = get_credential('TRINO_USER', 'trino')
TRINO_PASSWORD = get_credential('TRINO_PASSWORD', '')
TRINO_HOST = get_param('TRINO_HOST', 'localhost')
TRINO_PORT = get_param('TRINO_PORT', '28082')
TRINO_CATALOG = get_param('TRINO_CATALOG', 'minio')
TRINO_SCHEMA = get_param('TRINO_SCHEMA', 'default')
TRINO_USE_SSL = get_param('TRINO_USE_SSL', 'true').lower() in ('true', '1', 't')


# -------------------------------------------------
# Trino connection (adjust!)
def get_trino_connection():

    if TRINO_USE_SSL:
        http_scheme = "https"
    else:
        http_scheme = "http"

    # Construct connection URLs
    conn = trino.dbapi.connect(
        host=f"{TRINO_HOST}",
        port=int(TRINO_PORT),
        user=f"{TRINO_USER}",
        catalog=f"{TRINO_CATALOG}",
        schema=f"{TRINO_SCHEMA}",
        http_scheme=http_scheme,
        auth=BasicAuthentication(
            TRINO_USER,
            TRINO_PASSWORD
        ) if TRINO_PASSWORD else None,
        verify=False  # Disable SSL verification for self-signed certificates,
    )

    return conn

conn = get_trino_connection()

def fetch_trino_rows(sql):
    """
    Execute SQL and return list of dicts:
    [{col: value, ...}, ...]
    """
    cur = conn.cursor()
    cur.execute(sql)

    cols = [c[0] for c in cur.description]
    rows = []
    for row in cur.fetchall():
        rows.append(dict(zip(cols, row)))
    return rows

merge_records = fetch_trino_rows("SELECT * FROM benchmark_scd2_merge_report_v")
query_records = fetch_trino_rows("SELECT * FROM benchmark_scd2_query_report_v")

print (merge_records)

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 200)

# ———————————————————————————
# Functions to parse string lists into floats
def parse_list(v):
    """
    Handles:
    - list[str] coming directly from Trino
    - list[float]
    - string representations like "{'10.94','18.42'}" (legacy JSON case)
    """
    if v is None:
        return []

    # Already a Python list (Trino ARRAY)
    if isinstance(v, list):
        return [float(x) for x in v if x is not None]

    # Legacy JSON string case
    if isinstance(v, str):
        return [float(x) for x in ast.literal_eval(v)]

    raise TypeError(f"Unexpected value type: {type(v)}")

# ———————————————————————————
# --- MERGE SUMMARY ---
ts_keys = [f"es_ts_{i}_l" for i in range(1,16)]

merge_stats = {}
for ts in ts_keys:
    all_vals = []
    for day in merge_records:
        if ts in day:
            all_vals.extend(parse_list(day[ts]))
    arr = np.array(all_vals)

    if arr.size == 0:
        print("Skipping percentile computation: no values left after filtering")
        continue   # or return / set NaNs, depending on your logic

    Q1 = np.percentile(arr, 25)
    Q3 = np.percentile(arr, 75)
    IQR = Q3 - Q1

    lower = Q1 - 1.5 * IQR
    upper = Q3 + 1.5 * IQR
    filtered = arr[(arr >= lower) & (arr <= upper)]

    #zs = zscore(arr)
    #filtered = arr[np.abs(zs) < 3]   # keep only those with |z| < 3

    if (ts == "es_ts_2_m"):
        print("Filtered shape:", filtered.shape)
        print("Filtered values:", filtered)
        print("Original values:", arr)

        # Outliers = those not in filtered
        outliers = arr[(arr < lower) | (arr > upper)]
        print("Outliers removed:")
        print(outliers)

    merge_stats[ts] = {
        "count": len(filtered),
        "avg": np.mean(filtered),
        "median": np.median(filtered),
        "min": np.min(filtered),
        "max": np.max(filtered),
        "std": np.std(filtered),
    }

merge_df = pd.DataFrame(merge_stats).round(3)

print("\n==== Merge Summary Table ====")
print(merge_df)

# save CSVs
pd.DataFrame(merge_stats).round(3).to_csv(f"merge_summary.csv")

query_stats = {}

for rec in query_records:
    base = rec["base_statement_key"]

    for i in range(1, 19):   # es_ts_1 … es_ts_18
        ts_key = f"ts_{i}"
        col = f"es_ts_{i}"

        vals = rec.get(col)

        # skip NULL or empty arrays
        if not vals:
            continue

        # convert ARRAY<VARCHAR> → numpy float array
        arr = np.array(vals, dtype=float)

        # safety guard (prevents your IndexError)
        if arr.size < 2:
            continue

        # --- IQR outlier filtering ---
        Q1 = np.percentile(arr, 25)
        Q3 = np.percentile(arr, 75)
        IQR = Q3 - Q1

        lower = Q1 - 1.5 * IQR
        upper = Q3 + 1.5 * IQR
        filtered = arr[(arr >= lower) & (arr <= upper)]

        # if everything got filtered out, fall back to original
        if filtered.size == 0:
            filtered = arr

        stats = {
            "count": int(filtered.size),
            "avg": float(np.mean(filtered)),
            "median": float(np.median(filtered)),
            "min": float(np.min(filtered)),
            "max": float(np.max(filtered)),
            "std": float(np.std(filtered)),
        }

        query_stats.setdefault(base, {})[ts_key] = stats

# ———————————————————————————
# --- OUTPUT ---

for base, stats in query_stats.items():
    df = pd.DataFrame(stats).round(3)
    print(f"\n==== Query Summary: {base} ====")
    print(df)

# save CSVs
for base, stats in query_stats.items():
    pd.DataFrame(stats).round(3).to_csv(f"query_summary_{base}.csv")

print("\nSummary tables exported as CSV.")