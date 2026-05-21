# Unit Testing of Iceberg SCD2 Implementation

This folder holds various unit tests of the SCD2 Iceberg implementation. These tests are fine-granular and only a certain aspect is tested. Each aspect is in its own file.

To run a test perform (example of running `test_scd_ins.py`)

```
pytest ./test_scd_ins.py -v --log-cli-level=INFO -s -vv
```

**Overview of Unit Tests**

The last columns point to the markdown which is being generated in the test and shows both the initial data as well as the result in the SCD2 table.

## SCD2 (Full Source)

### Select

| # | Python Script | Description | Trino Report | Spark Report | PySpark Report |
|---|------|------------|---------------------|---------------------|---------------------|
| 1 | test_scd2_sel_point_in_time.py | Performs a point-in-time select to find the version of the entity which was valid at the time | [trino](./reports/trino/scd2_test_sel_point_in_time.md) | [spark](./reports/spark/scd2_test_sel_point_in_time.md) | [pyspark](./reports/pyspark/scd2_test_sel_point_in_time.md) |
| 2 | test_scd2_sel_is_active.py | Performs a select to get all active versions of all entities. | [trino](./reports/trino/scd2_test_sel_is_active.md) | [spark](./reports/spark/scd2_test_sel_is_active.md) | [pyspark](./reports/pyspark/scd2_test_sel_is_active.md) |
| 3 | test_scd2_sel_is_latest.py | Performs a select to get all latest versions of all entities. | [trino](./reports/trino/scd2_test_sel_is_latest.md) | [spark](./reports/spark/scd2_test_sel_is_latest.md) | [pyspark](./reports/pyspark/scd2_test_sel_is_latest.md) |

### Insert

| # | Python Script | Description | Trino Report | Spark Report | PySpark Report |
|---|------|------------|---------------------|---------------------|---------------------|
| 1 | test_scd2_ins_empty.py | Performs a single insert of entity into an empty dimension table | [trino](./reports/trino/scd2_test_ins_empty.md) | [spark](./reports/spark/scd2_test_ins_empty.md) | [pyspark](./reports/pyspark/scd2_test_ins_empty.md) |
| 2 | test_scd2_ins.py | Performs a single insert of a new entity into a non-empty dimension table | [trino](./reports/trino/scd2_test_ins.md) | [spark](./reports/spark/scd2_test_ins.md) | [pyspark](./reports/pyspark/scd2_test_ins.md) |
| 3 | test_scd2_intraday_ins.py | Performs multiple insert operations for the same entity within the same day (intraday) | [trino](./reports/trino/scd2_test_intraday_ins.md) | [spark](./reports/spark/scd2_test_intraday_ins.md) | [pyspark](./reports/pyspark/scd2_test_intraday_ins.md) |
| 4 | test_scd2_ins_ins_past_same_val.py | Performs an insert followed by a back-dated insert with the same value | [trino](./reports/trino/scd2_test_ins_ins_past_same_val.md) | [spark](./reports/spark/scd2_test_ins_ins_past_same_val.md) | [pyspark](./reports/pyspark/scd2_test_ins_ins_past_same_val.md) |
| 5 | test_scd2_ins_ins_past_diff_val.py | Performs an insert followed by a back-dated insert with a different value | [trino](./reports/trino/scd2_test_ins_ins_past_diff_val.md) | [spark](./reports/spark/scd2_test_ins_ins_past_diff_val.md) | [pyspark](./reports/pyspark/scd2_test_ins_ins_past_diff_val.md) |
| 6 | test_scd2_compkey_ins.py | Performs an insert of a new entity using a composite primary key | [trino](./reports/trino/scd2_test_compkey_ins.md) | [spark](./reports/spark/scd2_test_compkey_ins.md) | [pyspark](./reports/pyspark/scd2_test_compkey_ins.md) |
| 7 | test_scd2_compkeynull_ins.py | Performs an insert of a new entity using a composite primary key containing NULL values | [trino](./reports/trino/scd2_test_compkeynull_ins.md) | [spark](./reports/spark/scd2_test_compkeynull_ins.md) | [pyspark](./reports/pyspark/scd2_test_compkeynull_ins.md) |

### Update

| # | Python Script | Description | Trino Report | Spark Report | PySpark Report |
|---|------|------------|---------------------|---------------------|---------------------|
| 1 | test_scd2_upd.py | Performs a single update of one entity | [trino](./reports/trino/scd2_test_upd.md) | [spark](./reports/spark/scd2_test_upd.md) | [pyspark](./reports/pyspark/scd2_test_upd.md) |
| 2 | test_scd2_upd_upd.py | Performs multiple updates on a single entity over time | [trino](./reports/trino/scd2_test_upd_upd.md) | [spark](./reports/spark/scd2_test_upd_upd.md) | [pyspark](./reports/pyspark/scd2_test_upd_upd.md) |
| 3 | test_scd2_upd_two_entities.py | Performs a single update of two entities | [trino](./reports/trino/scd2_test_upd_two_entities.md) | [spark](./reports/spark/scd2_test_upd_two_entities.md) | [pyspark](./reports/pyspark/scd2_test_upd_two_entities.md) |
| 4 | test_scd2_upd_past_diff_val.py | Performs a back-dated correction of an entity with a different value | [trino](./reports/trino/scd2_test_upd_past_diff_val.md) | [spark](./reports/spark/scd2_test_upd_past_diff_val.md) | [pyspark](./reports/pyspark/scd2_test_upd_past_diff_val.md) |
| 5 | test_scd2_upd_past_same_val.py | Performs a back-dated correction of an entity with the same value (no effective change) | [trino](./reports/trino/scd2_test_upd_past_same_val.md) | [spark](./reports/spark/scd2_test_upd_past_same_val.md) | [pyspark](./reports/pyspark/scd2_test_upd_past_same_val.md) |
| 6 | test_scd2_compkey_upd.py | Performs an update of an entity using a composite primary key | [trino](./reports/trino/scd2_test_compkey_upd.md) | [spark](./reports/spark/scd2_test_compkey_upd.md) | [pyspark](./reports/pyspark/scd2_test_compkey_upd.md) |

### Logical Delete

| # | Python Script | Description | Trino Report | Spark Report | PySpark Report |
|---|------|------------|---------------------|---------------------|---------------------|
| 1 | test_scd2_logical_del.py | Performs a single delete of entity using logical delete operation in the input table | [trino](./reports/trino/scd2_test_logical_del.md) | [spark](./reports/spark/scd2_test_logical_del.md) | [pyspark](./reports/pyspark/scd2_test_logical_del.md) |
| 2 | test_scd2_logical_del_del.py | Performs a logical delete twice to validate idempotency | [trino](./reports/trino/scd2_test_logical_del_del.md) | [spark](./reports/spark/scd2_test_logical_del_del.md) | [pyspark](./reports/pyspark/scd2_test_logical_del_del.md) |
| 3 | test_scd2_logical_del_with_many_versions.py | Performs a single delete of an entity (with many versions), using logical delete operation in the input table | [trino](./reports/trino/scd2_test_logical_del_with_many_versions.md) | [spark](./reports/spark/scd2_test_logical_del_with_many_versions.md) | [pyspark](./reports/pyspark/scd2_test_logical_del_with_many_versions.md) |
| 4 | test_scd2_logical_del_del_past.py | Performs a logical delete followed by a back-dated delete | [trino](./reports/trino/scd2_test_logical_del_del_past.md) | [spark](./reports/spark/scd2_test_logical_del_del_past.md) | [pyspark](./reports/pyspark/scd2_test_logical_del_del_past.md) |
| 5 | test_scd2_logical_del_and_reins_same_val.py | Performs a Re-Insert into an entity which has previously been deleted. Insert with same values and no gap. | [trino](./reports/trino/scd2_test_logical_del_and_ins_same_val.md) | [spark](./reports/spark/scd2_test_logical_del_and_ins_same_val.md) | [pyspark](./reports/pyspark/scd2_test_logical_del_and_ins_same_val.md) |
| 6 | test_scd2_logical_del_and_reins_diff_val.py | Performs a Re-Insert into an entity which has previously been deleted. Insert with different values and no gap. | [trino](./reports/trino/scd2_test_logical_del_and_reins_diff_val.md) | [spark](./reports/spark/scd2_test_logical_del_and_reins_diff_val.md) | [pyspark](./reports/pyspark/scd2_test_logical_del_and_reins_diff_val.md) |
| 7 | test_scd2_logical_del_and_reins_with_overlap_same_val.py | Performs a re-insert after a logical delete with an overlapping timestamp and the same value | [trino](./reports/trino/scd2_test_logical_del_and_reins_with_overlap_same_val.md) | [spark](./reports/spark/scd2_test_logical_del_and_reins_with_overlap_same_val.md) | [pyspark](./reports/pyspark/scd2_test_logical_del_and_reins_with_overlap_same_val.md) |
| 8 | test_scd2_logical_del_and_reins_with_overlap_diff_val.py | Performs a re-insert after a logical delete with an overlapping timestamp and a different value | [trino](./reports/trino/scd2_test_logical_del_and_reins_with_overlap_diff_val.md) | [spark](./reports/spark/scd2_test_logical_del_and_reins_with_overlap_diff_val.md) | [pyspark](./reports/pyspark/scd2_test_logical_del_and_reins_with_overlap_diff_val.md) |
| 9 | test_scd2_logical_del_and_reins_with_gap_same_val.py | Performs a re-insert after a logical delete with a time gap and the same value | [trino](./reports/trino/scd2_test_logical_del_and_reins_with_gap_same_val.md) | [spark](./reports/spark/scd2_test_logical_del_and_reins_with_gap_same_val.md) | [pyspark](./reports/pyspark/scd2_test_logical_del_and_reins_with_gap_same_val.md) |
| 10 | test_scd2_logical_del_and_reins_with_gap_diff_val.py | Performs a re-insert after a logical delete with a time gap and a different value | [trino](./reports/trino/scd2_test_logical_del_and_reins_with_gap_diff_val.md) | [spark](./reports/spark/scd2_test_logical_del_and_reins_with_gap_diff_val.md) | [pyspark](./reports/pyspark/scd2_test_logical_del_and_reins_with_gap_diff_val.md) |


### Physical Delete

| # | Python Script | Description | Trino Report | Spark Report | PySpark Report |
|---|------|------------|---------------------|---------------------|---------------------|
| 1 | test_scd2_del.py | Performs a physical delete operation on an entity | [trino](./reports/trino/scd2_test_del.md) | [spark](./reports/spark/scd2_test_del.md) | [pyspark](./reports/pyspark/scd2_test_del.md) |
| 2 | test_scd2_full_del_checkscd2.py | Performs a physical delete operation by checking against the SCD2 table | [trino](./reports/trino/scd2_test_full_del_checkscd2.md) | [spark](./reports/spark/scd2_test_full_del_checkscd2.md) | [pyspark](./reports/pyspark/scd2_test_full_del_checkscd2.md) |

### Fill Gap

| # | Python Script | Description | Trino Report | Spark Report | PySpark Report |
|---|------|------------|---------------------|---------------------|---------------------|
| 1 | test_scd2_fill_gap_same_val_next.py | Fills a gap in the timeline with the same value as the version after the gap | [trino](./reports/trino/scd2_test_fill_gap_same_val_next.md) | [spark](./reports/spark/scd2_test_fill_gap_same_val_next.md) | [pyspark](./reports/pyspark/scd2_test_fill_gap_same_val_next.md) |
| 2 | test_scd2_fill_gap_same_val_prev.py | Fills a gap in the timeline with the same value as the version before the gap | [trino](./reports/trino/scd2_test_fill_gap_same_val_prev.md) | [spark](./reports/spark/scd2_test_fill_gap_same_val_prev.md) | [pyspark](./reports/pyspark/scd2_test_fill_gap_same_val_prev.md) |
| 3 | test_scd2_fill_gap_diff_val_prev_and_next.py | Fills a gap in the timeline with a different value than the versions before and after the gap | [trino](./reports/trino/scd2_test_fill_gap_diff_val_prev_and_next.md) | [spark](./reports/spark/scd2_test_fill_gap_diff_val_prev_and_next.md) | [pyspark](./reports/pyspark/scd2_test_fill_gap_diff_val_prev_and_next.md) |
| 4 | test_scd2_fill_gap_same_val_prev_and_next.py | Fills a gap in the timeline with the same value as both the version before and after the gap | [trino](./reports/trino/scd2_test_fill_gap_same_val_prev_and_next.md) | [spark](./reports/spark/scd2_test_fill_gap_same_val_prev_and_next.md) | [pyspark](./reports/pyspark/scd2_test_fill_gap_same_val_prev_and_next.md) |
| 5 | test_scd2_fill_gap_partial_same_val_next.py | Partially fills a gap in the timeline with the same value as the version after the gap | [trino](./reports/trino/scd2_test_fill_gap_partial_same_val_next.md) | [spark](./reports/spark/scd2_test_fill_gap_partial_same_val_next.md) | [pyspark](./reports/pyspark/scd2_test_fill_gap_partial_same_val_next.md) |
| 6 | test_scd2_fill_gap_partial_diff_val_next.py | Partially fills a gap in the timeline with a different value than the version after the gap | [trino](./reports/trino/scd2_test_fill_gap_partial_diff_val_next.md) | [spark](./reports/spark/scd2_test_fill_gap_partial_diff_val_next.md) | [pyspark](./reports/pyspark/scd2_test_fill_gap_partial_diff_val_next.md) |

### Misc

| # | Python Script | Description | Trino Report | Spark Report | PySpark Report |
|---|------|------------|---------------------|---------------------|---------------------|
| 1 | test_scd2_replay.py | Validates multiple operations over time and replays all of them a second time | [trino](./reports/trino/scd2_test_replay.md) | [spark](./reports/spark/scd2_test_replay.md) | [pyspark](./reports/pyspark/scd2_test_replay.md) |
| 2 | test_scd2_non_overlapping_ts.py | Validates that adding a new version for an entity results in non-overlapping timestamps | [trino](./reports/trino/scd2_test_non_overlapping_ts.md) | - | [pyspark](./reports/pyspark/scd2_test_non_overlapping_ts.md) |

## SCD2 (Delta Source)

| # | Python Script | Description | Trino Report | Spark Report | PySpark Report |
|---|------|------------|---------------------|---------------------|---------------------|
| 1 | test_scd2_delta_ins_empty.py | Performs a single insert of entity into an empty dimension table | [trino](./reports/trino/scd2_delta_test_ins_empty.md) | [spark](./reports/spark/scd2_delta_test_ins_empty.md) | [pyspark](./reports/pyspark/scd2_delta_test_ins_empty.md) |
| 2 | test_scd2_delta_ins.py | Performs a single insert of a new entity into a non-empty dimension table | [trino](./reports/trino/scd2_delta_test_ins.md) | [spark](./reports/spark/scd2_delta_test_ins.md) | [pyspark](./reports/pyspark/scd2_delta_test_ins.md) |
| 3 | test_scd2_delta_logical_del.py | Performs a single delete of entity using logical delete operation in the input table | [trino](./reports/trino/scd2_delta_test_logical_del.md) | [spark](./reports/spark/scd2_delta_test_logical_del.md) | [pyspark](./reports/pyspark/scd2_delta_test_logical_del.md) |
| 4 | test_scd2_delta_streaming.py | Performs a single delete of entity using logical delete operation in the input table | [trino](./reports/trino/scd2_delta_test_streaming.md) | [spark](./reports/spark/scd2_delta_test_streaming.md) | [pyspark](./reports/pyspark/scd2_delta_test_streaming.md) |
