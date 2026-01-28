import json
import statistics
import ast
import pandas as pd
import numpy as np
from scipy.stats import zscore

merge_records = json.load(open("benchmark_merge_l_cust.json"))["select * from benchmark_scd2_merge_report_v"]
query_records = json.load(open("benchmark_select_l_cust.json"))["select * from benchmark_scd2_query_report_v"]

print(merge_records)

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 200)

# ———————————————————————————
# Functions to parse string lists into floats
def parse_list(s):
    return [float(x) for x in ast.literal_eval(s)]

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

    Q1 = np.percentile(arr, 25)
    Q3 = np.percentile(arr, 75)
    IQR = Q3 - Q1

    lower = Q1 - 1.5 * IQR
    upper = Q3 + 1.5 * IQR
    filtered = arr[(arr >= lower) & (arr <= upper)]

    #zs = zscore(arr)
    #filtered = arr[np.abs(zs) < 3]   # keep only those with |z| < 3

    if (ts == "es_ts_2_l"):
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

# ———————————————————————————
# --- QUERY SUMMARY ---
# collect stats for each base strategy + ts combination
query_stats = {}

for rec in query_records:
    base = rec["base_strategy"]
    for i in range(1, 16):
        key = f"ts_{i}"
        value_key = f"es_ts_{i}"
        if value_key in rec:
            vals = parse_list(rec[value_key])
            arr = np.array(vals)
            
            Q1 = np.percentile(arr, 25)
            Q3 = np.percentile(arr, 75)
            IQR = Q3 - Q1

            lower = Q1 - 1.5 * IQR
            upper = Q3 + 1.5 * IQR
            filtered = arr[(arr >= lower) & (arr <= upper)]

            stats = {
                "count": len(filtered),
                "avg": np.mean(filtered),
                "median": np.median(filtered),
                "min": np.min(filtered),
                "max": np.max(filtered),
                "std": np.std(filtered)
            }
            query_stats.setdefault(base, {})[key] = stats

# create a separate table per base strategy
for base, stats in query_stats.items():
    df = pd.DataFrame(stats).round(3)
    print(f"\n==== Query Summary: {base} ====")
    print(df)

# Save to CSV optionally
merge_df.to_csv("merge_summary.csv")
for base, stats in query_stats.items():
    pd.DataFrame(stats).to_csv(f"query_summary_{base}.csv")

print("\nSummary tables exported as CSV.")