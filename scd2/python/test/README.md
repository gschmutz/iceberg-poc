# Unit Testing of Iceberg SCD2 Implementation

This folder holds various unit tests of the SCD2 Iceberg implementation. These tests are fine-granular and only a certain aspect is tested. Each aspect is in its own file. 

To run a test perform (example of running `test_scd_ins.py`)

```
pytest ./test_scd_ins.py -v --log-cli-level=INFO -s -vv
```

## Overview of Unit Tests

The last column points to the markdown which is being generated in the test and shows both the initial data as well as the result in the SCD2 table.

### Generic Iceberg

| # | Python Script  |   Description | Link to Results as Markdown | Use Asserts |
|---|------|------------|---------------------|-------|
| 1 | test_iceberg_table_add_col.py | Add a column to an existing table and time travel to system time before. | [test_iceberg_table_add_col.md](./reports/test_iceberg_table_add_col.md) | yes | 
| 2 | test_iceberg_table_rename.py | Rename an Iceberg table. | [tst_iceberg_table_rename.md](./reports/test_iceberg_table_rename.md) | no | 
| 3 | test_iceberg_table_as_of.py | Test time travel in Iceberg table with the VERSION AS OF clause | [test_iceberg_table_as_of.md](./reports/test_iceberg_table_as_of.md) | yes | 
| 4 | test_iceberg_optimize.py | Optimize an iceberg table so that many small files are merged into larger ones. | [test_iceberg_optimize.md](./reports/test_iceberg_optimize.md) | no | 


### SCD2

| # | Python Script  |   Description | Link to Results as Markdown | Use Asserts |
|---|------|------------|---------------------|----|
| 1 | test_scd2_ins_empty.py | Performs a single insert of entity into an empty dimension table | [scd2_test_ins_empty.md](./reports/scd2_test_ins_empty.md) | yes |
| 2 | test_scd2_ins.py | Performs a single insert of a new entity into a non-empty dimension table | [scd2_tests_ins.md](./reports/scd2_test_ins.md) | yes |
| 3 | test_scd2_upd.py | Performs a single update of one entity | [scd2_test_upd.md](./reports/scd2_test_upd.md) | yes |
| 4 | test_scd2_upd_two_entities.py | Performs a single update of two entities | [scd2_test_up_two_entities_.md](./reports/scd2_test_upd_two_entities.md) | yes |
| 5 | test_scd2_del.py | Performs a single delete of entity (with only one version) using physical delete operation in the input table | [scd2_test_del.md](./reports/scd2_test_logical_del.md) | yes
| 6 | test_scd2_del_with_many_versions.py | Performs a single delete of an entity (with many versions), using physical delete operation in the input table | [scd2_test_del_with_many_versions.md](./reports/scd2_test_del_with_many_versions.md) | yes
| 7 | test_scd2_logical_del.py | Performs a single delete of entity using logical delete operation in the input table | [scd2_test_logical_del.md](./reports/scd2_test_logical_del.md) | yes
| 8 | test_scd2_logical_del_with_many_versions.py | Performs a single delete of an entity (with many versions), using logical delete operation in the input table | [scd2_test_logical_del_with_many_versions.md](./reports/scd2_test_logical_del_with_many_versions.md) | yes
| 9 | test_scd2_upd_upd_.py | Performs multiple updates on a single entity over time | [scd2_test_upd_upd.md](./reports/scd2_test_upd_upd.md) | yes |
| 10 | test_scd2_sel_point_in_time.py | Performs a point-in-time select to find the version of the entity which was valid at the time | [scd2_sel_point_in_time.md](./reports/scd2_test_sel_point_in_time.md) | no |
| 11 | test_scd2_sel_is_active.py | Performs a select to get all active versions of all entities. | [scd2_test_sel_is_active.md](./reports/scd2_test_sel_is_active.md) | no |
| 12 | test_scd2_sel_is_latest.py | Performs a select to get all latest versions of all entities. | [scd2_test_sel_is_latest.md](./reports/scd2_test_sel_is_latest.md) | no |
| 99 | test_scd2_e2e_.py | Performs and end-to-end test with multiple steps and operations | [scd2_test_upd_upd.md](./reports/scd2_test_e2e.md) | no |

