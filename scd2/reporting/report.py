import json
import statistics
import ast
import pandas as pd


merge_data = json.load(open("benchmark_merge_l_cust.json"))["select * from iceberg.default.benchmark_scd2_merge_report_v"]
query_data = json.load(open("benchmark_select_l_cust.json"))["select * from iceberg.default.benchmark_scd2_query_report_v"]

# helper to parse string lists
def parse_list(s):
    return [float(x) for x in ast.literal_eval(s)]

# collect merge stats
merge_stats = {}
for ts in range(1, 16):
    key = f"es_ts_{ts}_l"
    all_values = []
    for day in merge_data:
        all_values += parse_list(day[key])
    merge_stats[f"ts_{ts}"] = {
        "merge_mean": statistics.mean(all_values),
        "merge_median": statistics.median(all_values),
        "merge_min": min(all_values),
        "merge_max": max(all_values),
        "merge_std": statistics.pstdev(all_values),
    }

# collect query stats by ts
query_stats = {f"ts_{i}": {} for i in range(1, 16)}

# assume query_data contains 4 repeated query groups per day
for entry in query_data:
    for ts in range(1, 16):
        key = f"es_ts_{ts}"
        values = parse_list(entry[key])
        if f"query_all" not in query_stats[f"ts_{ts}"]:
            query_stats[f"ts_{ts}"]["query_all"] = []
        query_stats[f"ts_{ts}"]["query_all"] += values

# compute query stats
for ts, stats in query_stats.items():
    vals = stats["query_all"]
    stats.update({
        "query_mean": statistics.mean(vals),
        "query_median": statistics.median(vals),
        "query_min": min(vals),
        "query_max": max(vals),
        "query_std": statistics.pstdev(vals),
    })

# combine into a table
df = pd.DataFrame({
    ts: {
        **merge_stats[ts],
        **query_stats[ts]
    }
    for ts in merge_stats
})

print(df.round(3).T)
df.to_csv("scd2_summary.csv")
print("Summary saved to scd2_summary.csv")