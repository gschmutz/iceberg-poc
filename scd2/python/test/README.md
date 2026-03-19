# Unit Testing of Iceberg SCD2 Implementation

This folder holds various unit tests of the SCD2 Iceberg implementation. These tests are fine-granular and only a certain aspect is tested. Each aspect is in its own file. 

To run a test perform (example of running `test_scd_ins.py`)

```
pytest ./test_scd_ins.py -v --log-cli-level=INFO -s -vv
```

**Overview of Unit Tests**

The last column points to the markdown which is being generated in the test and shows both the initial data as well as the result in the SCD2 table.

## Generic Iceberg

| # | Python Script  |   Description | Link to Results as Markdown | Use Asserts |
|---|------|------------|---------------------|-------|
| 1 | test_iceberg_table_add_col.py | Add a column to an existing table and time travel to system time before. | [test_iceberg_table_add_col.md](./reports/trino/test_iceberg_table_add_col.md) | yes | 
| 2 | test_iceberg_table_rename.py | Rename an Iceberg table. | [tst_iceberg_table_rename.md](./reports/trino/test_iceberg_table_rename.md) | no | 
| 3 | test_iceberg_table_as_of.py | Test time travel in Iceberg table with the VERSION AS OF clause | [test_iceberg_table_as_of.md](./reports/trino/test_iceberg_table_as_of.md) | yes | 
| 4 | test_iceberg_optimize.py | Optimize an iceberg table so that many small files are merged into larger ones. | [test_iceberg_optimize.md](./reports/trino/test_iceberg_optimize.md) | no | 


## SCD2 (Full Source)

### Select

| # | Python Script  |   Description | Link to Results as Markdown | Use Asserts |
|---|------|------------|---------------------|----|
| 1 | test_scd2_sel_point_in_time.py | Performs a point-in-time select to find the version of the entity which was valid at the time | [scd2_sel_point_in_time.md](./reports/trino/scd2_test_sel_point_in_time.md) | no |
| 2 | test_scd2_sel_is_active.py | Performs a select to get all active versions of all entities. | [scd2_test_sel_is_active.md](./reports/trino/scd2_test_sel_is_active.md) | no |
| 3 | test_scd2_sel_is_latest.py | Performs a select to get all latest versions of all entities. | [scd2_test_sel_is_latest.md](./reports/trino/scd2_test_sel_is_latest.md) | no |

### Insert

| # | Python Script  |   Description | Link to Results as Markdown | Use Asserts |
|---|------|------------|---------------------|----|
| 1 | test_scd2_ins_empty.py | Performs a single insert of entity into an empty dimension table | [scd2_test_ins_empty.md](./reports/trino/scd2_test_ins_empty.md) | yes |
| 2 | test_scd2_ins.py | Performs a single insert of a new entity into a non-empty dimension table | [scd2_tests_ins.md](./reports/trino/scd2_test_ins.md) | yes |
| 3 | test_scd2_intraday_ins.py | Performs multiple insert operations for the same entity within the same day (intraday) | [scd2_test_intraday_ins.md](./reports/trino/scd2_test_intraday_ins.md) | yes |
| 4 | test_scd2_ins_ins_past_same_val.py | Performs an insert followed by a back-dated insert with the same value | [scd2_test_ins_ins_past_same_val.md](./reports/trino/scd2_test_ins_ins_past_same_val.md) | yes |
| 5 | test_scd2_ins_ins_past_diff_val.py | Performs an insert followed by a back-dated insert with a different value | [scd2_test_ins_ins_past_diff_val.md](./reports/trino/scd2_test_ins_ins_past_diff_val.md) | yes |
| 6 | test_scd2_compkey_ins.py | Performs an insert of a new entity using a composite primary key | [scd2_test_compkey_ins.md](./reports/trino/scd2_test_compkey_ins.md) | yes |
| 7 | test_scd2_compkeynull_ins.py | Performs an insert of a new entity using a composite primary key containing NULL values | [scd2_test_compkeynull_ins.md](./reports/trino/scd2_test_compkeynull_ins.md) | yes |

### Update

| # | Python Script  |   Description | Link to Results as Markdown | Use Asserts |
|---|------|------------|---------------------|----|
| 1 | test_scd2_upd.py | Performs a single update of one entity | [scd2_test_upd.md](./reports/trino/scd2_test_upd.md) | yes |
| 2 | test_scd2_upd_upd_.py | Performs multiple updates on a single entity over time | [scd2_test_upd_upd.md](./reports/trino/scd2_test_upd_upd.md) | yes |
| 3 | test_scd2_upd_two_entities.py | Performs a single update of two entities | [scd2_test_up_two_entities_.md](./reports/trino/scd2_test_upd_two_entities.md) | yes |
| 4 | test_scd2_upd_past_diff_val.py | Performs a back-dated correction of an entity with a different value | [scd2_test_upd_past_diff_val.md](./reports/trino/scd2_test_upd_past_diff_val.md) | yes |
| 5 | test_scd2_upd_past_same_val.py | Performs a back-dated correction of an entity with the same value (no effective change) | [scd2_test_upd_past_same_val.md](./reports/trino/scd2_test_upd_past_same_val.md) | yes |
| 6 | test_scd2_compkey_upd.py | Performs an update of an entity using a composite primary key | [scd2_test_compkey_upd.md](./reports/trino/scd2_test_compkey_upd.md) | yes |

### Logical Delete

| # | Python Script  |   Description | Link to Results as Markdown | Use Asserts |
|---|------|------------|---------------------|----|
| 1 | test_scd2_logical_del.py | Performs a single delete of entity using logical delete operation in the input table | [scd2_test_logical_del.md](./reports/trino/scd2_test_logical_del.md) | yes
| 2 | test_scd2_logical_del_del.py | Performs a logical delete twice to validate idempotency | [scd2_test_logical_del_del.md](./reports/trino/scd2_test_logical_del_del.md) | yes |
| 3 | test_scd2_logical_del_with_many_versions.py | Performs a single delete of an entity (with many versions), using logical delete operation in the input table | [scd2_test_logical_del_with_many_versions.md](./reports/trino/scd2_test_logical_del_with_many_versions.md) | yes
| 4 | test_scd2_logical_del_del_past.py | Performs a logical delete followed by a back-dated delete | [scd2_test_logical_del_del_past.md](./reports/trino/scd2_test_logical_del_del_past.md) | yes |
| 5 | test_scd2_logical_del_and_reins_same_val.py | Performs a Re-Insert into an entity which has previously been deleted. Insert with same values and no gap. | [scd2_test_logical_del_and_reins_same.md](./reports/trino/scd2_test_logical_logical_del_and_reins_same.md) | yes |
| 6 | test_scd2_logical_del_and_reins_diff_val.py | Performs a Re-Insert into an entity which has previously been deleted. Insert with different values and no gap. | [scd2_test_logical_del_and_reins_diff.md](./reports/trino/scd2_test_logical_del_and_reins_diff.md) | yes |
| 7 | test_scd2_logical_del_and_reins_with_overlap_same_val.py | Performs a re-insert after a logical delete with an overlapping timestamp and the same value | [scd2_test_logical_del_and_reins_with_overlap_same_val.md](./reports/trino/scd2_test_logical_del_and_reins_with_overlap_same_val.md) | yes |
| 8 | test_scd2_logical_del_and_reins_with_overlap_diff_val.py | Performs a re-insert after a logical delete with an overlapping timestamp and a different value | [scd2_test_logical_del_and_reins_with_overlap_diff_val.md](./reports/trino/scd2_test_logical_del_and_reins_with_overlap_diff_val.md) | yes |
| 9 | test_scd2_logical_del_and_reins_with_gap_same_val.py | Performs a re-insert after a logical delete with a time gap and the same value | [scd2_test_logical_del_and_reins_with_gap_same_val.md](./reports/trino/scd2_test_logical_del_and_reins_with_gap_same_val.md) | yes |
| 10 | test_scd2_logical_del_and_reins_with_gap_diff_val.py | Performs a re-insert after a logical delete with a time gap and a different value | [scd2_test_logical_del_and_reins_with_gap_diff_val.md](./reports/trino/scd2_test_logical_del_and_reins_with_gap_diff_val.md) | yes |


### Fill Gap

| # | Python Script  |   Description | Link to Results as Markdown | Use Asserts |
|---|------|------------|---------------------|----|
| 1 | test_scd2_fill_gap_same_val_next.py | Fills a gap in the timeline with the same value as the version after the gap | [scd2_test_fill_gap_same_val_next.md](./reports/trino/scd2_test_fill_gap_same_val_next.md) | yes |
| 2 | test_scd2_fill_gap_same_val_prev.py | Fills a gap in the timeline with the same value as the version before the gap | [scd2_test_fill_gap_same_val_prev.md](./reports/trino/scd2_test_fill_gap_same_val_prev.md) | yes |
| 3 | test_scd2_fill_gap_diff_val_prev_and_next.py | Fills a gap in the timeline with a different value than the versions before and after the gap | [scd2_test_fill_gap_diff_val_prev_and_next.md](./reports/trino/scd2_test_fill_gap_diff_val_prev_and_next.md) | yes |
| 4 | test_scd2_fill_gap_same_val_prev_and_next.py | Fills a gap in the timeline with the same value as both the version before and after the gap | [scd2_test_fill_gap_same_val_prev_and_next.md](./reports/trino/scd2_test_fill_gap_same_val_prev_and_next.md) | yes |
| 5 | test_scd2_fill_gap_partial_same_val_next.py | Partially fills a gap in the timeline with the same value as the version after the gap | [scd2_test_fill_gap_partial_same_val_next.md](./reports/trino/scd2_test_fill_gap_partial_same_val_next.md) | yes |
| 6 | test_scd2_fill_gap_partial_diff_val_next.py | Partially fills a gap in the timeline with a different value than the version after the gap | [scd2_test_fill_gap_partial_diff_val_next.md](./reports/trino/scd2_test_fill_gap_partial_diff_val_next.md) | yes |

### Misc

| # | Python Script  |   Description | Link to Results as Markdown | Use Asserts |
|---|------|------------|---------------------|----|
| 1 | test_scd2_replay.py | Validates multiple operations over time and replays all of them a second time | [scd2_test_replay.md](./reports/trino/scd2_test_replay.md) | yes |


## SCD2 (Delta Source)

| # | Python Script  |   Description | Link to Results as Markdown | Use Asserts |
|---|------|------------|---------------------|----|
| 1 | test_scd2_delta_ins_empty.py | Performs a single insert of entity into an empty dimension table | [scd2_delta_test_ins_empty.md](./reports/trino/scd2_delta_test_ins_empty.md) | yes
| 2 | test_scd2_delta_ins.py | Performs a single insert of a new entity into a non-empty dimension table | [scd2_delta_test_ins.md](./reports/trino/scd2_delta_test_ins.md) | yes |
| 3 | test_scd2_delta_logical_del.py | Performs a single delete of entity using logical delete operation in the input table | [scd2_delta_test_logical_del.md](./reports/trino/scd2_delta_test_logical_del.md) | yes
| 4 | test_scd2_delta_streaming.py | Performs a single delete of entity using logical delete operation in the input table | [scd2_delta_test_streaming.md](./reports/trino/scd2_delta_test_streaming.md) | yes