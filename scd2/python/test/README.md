# Unit Testing of Iceberg SCD2 Implementation

This folder holds various unit tests of the SCD2 Iceberg implementation. These tests are fine-granular and only a certain aspect is tested. Each aspect is in its own file. 

To run a test perform (example of running `test_scd_ins.py`)

```
pytest ./test_scd_ins.py -v --log-cli-level=INFO -s -vv
```

## Overview of Unit Tests

The last column points to the markdown which is being generated in the test and shows both the initial data as well as the result in the SCD2 table.

### Generic Iceberg

| # | Python Script  |   Description | Link to Results as Markdown |
|---|------|------------|---------------------|
| 1 | test_iceberg_table_add_col.py | Add a column to an existing table and time travel to system time before. | [test_iceberg_add_col.md](./reports/test_iceberg_add_col.md) |
| 2 | test_iceberg_table_rename.py | Rename an Iceberg table. | [tst_iceberg_table_rename.md](./reports/test_iceberg_table_rename.md) |
| 3 | test_iceberg_optimize.py | Optimize an iceberg table so that many small files are merged into larger ones. | [test_iceberg_optimize.md](./reports/test_iceberg_optimize.md) |

### SCD2

| # | Python Script  |   Description | Link to Results as Markdown |
|---|------|------------|---------------------|
| 1 | test_scd2_ins.py | Performs a single insert of entity | [scd2_test_ins.md](./reports/scd2_test_ins.md) |
| 2 | test_scd2_upd.py | Performs a single update of entity | [scd2_test_upd.md](./reports/scd2_test_upd.md) |
| 3 | test_scd2_del.py | Performs a single delete of entity using physical delete operation | [scd2_test_del.md](./reports/scd2_test_logical_del.md) |
| 4 | test_scd2_logical_del.py | Performs a single delete of entity using logical delete operation | [scd2_test_logical_del.md](./reports/scd2_test_logical_del.md) |
| 5 | test_scd2_upd_upd_.py | Performs multiple updates on a single entity over time | [scd2_test_upd_upd.md](./reports/scd2_test_upd_upd.md) |
| 6 | test_scd2_sel.py | Performs a point-in-time select to find the version of the entity which was valid at the time | [scd2_test_sel.md](./reports/scd2_test_sel.md) |
| 7 | test_scd2_sel_is_active.py | Performs a select to get all active versions of all entities. | [scd2_test_sel_is_active.md](./reports/scd2_test_sel_is_active.md) |
| 8 | test_scd2_sel_is_latest.py | Performs a select to get all latest versions of all entities. | [scd2_test_sel_is_latest.md](./reports/scd2_test_sel_is_latest.md) |
| 99 | test_scd2_e2e_.py | Performs and end-to-end test with multiple steps and operations | [scd2_test_upd_upd.md](./reports/scd2_test_e2e.md) |

