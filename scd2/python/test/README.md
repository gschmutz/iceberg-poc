# Unit Testing of Iceberg SCD2 Implementation

This folder holds various unit tests of the SCD2 Iceberg implementation. These tests are fine-granular and only a certain aspect is tested. Each aspect is in its own file. 

To run a test perform (example of running `test_scd_ins.py`)

```
pytest ./test_scd_ins.py -v --log-cli-level=INFO -s -vv
```

## Overview of Unit Tests

The last column points to the markdown which is being generated in the test and shows both the initial data as well as the result in the SCD2 table.

| # | Python Script  |   Description | Link to Results as Markdown |
|---|------|------------|---------------------|
| 1 | test_scd2_ins.py | Performs a single insert of entity | [scd2_test_ins.md](./reports/scd2_test_ins.md) |
| 2 | test_scd2_upd.py | Performs a single update of entity | [scd2_test_upd.md](./reports/scd2_test_upd.md) |
| 3 | test_scd2_del.py | Performs a single delete of entity using physical delete operation | [scd2_test_del.md](./reports/scd2_test_logical_del.md) |
| 4 | test_scd2_logical_del.py | Performs a single delete of entity using logical delete operation | [scd2_test_logical_del.md](./reports/scd2_test_logical_del.md) |
| 5 | test_scd2_upd_upd_.py | Performs multiple updates on a single entity over time | [scd2_test_upd_upd.md](./reports/scd2_test_upd_upd.md) |
| 99 | test_scd2_e2e_.py | Performs and end-to-end test with multiple steps and operations | [scd2_test_upd_upd.md](./reports/scd2_test_e2e.md) |

