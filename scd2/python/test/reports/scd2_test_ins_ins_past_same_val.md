# Testing Insert Operation followed by an Insert Operation in the past

This test validates an INSERT operation in the past of a version for an entity which already exists and with the same value.
## Test Step 1
At 2026-01-05 00:00:00, insert 3 entities into raw table and perform initial SCD2 merge.


**Raw Table `raw_person`**


|   id | first_name   | last_name   | city   | email                    | status   | dp_ts_from          | dp_loaded_at        |
|------|--------------|-------------|--------|--------------------------|----------|---------------------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |



**Input to Merge**


| merge_key   | dp_key   |   id | first_name   | last_name   | city   | email                    | src_dp_ts_from      | load_ts             | status   | change_classification   | operation_type   | tgt_dp_ts_from      | tgt_dp_ts_to        | prev_dp_ts_from   | prev_dp_ts_to   | succ_dp_ts_from   | succ_dp_ts_to   |
|-------------|----------|------|--------------|-------------|--------|--------------------------|---------------------|---------------------|----------|-------------------------|------------------|---------------------|---------------------|-------------------|-----------------|-------------------|-----------------|
|             |          |    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 | ACTIVE   | NEW                     | UPDATE_EXISTING  | 9999-12-31 23:59:59 | 9999-12-31 23:59:59 |                   |                 |                   |                 |
|             |          |    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 | ACTIVE   | NEW                     | UPDATE_EXISTING  | 9999-12-31 23:59:59 | 9999-12-31 23:59:59 |                   |                 |                   |                 |
|             |          |    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 | ACTIVE   | NEW                     | UPDATE_EXISTING  | 9999-12-31 23:59:59 | 9999-12-31 23:59:59 |                   |                 |                   |                 |



**Dimensional Table `dim_person`**


| dp_key                                                                  | id                                   | first_name                               | last_name                                 | city                                      | email                                                       | dp_ts_from                                             | dp_ts_to                                               | dp_is_active                            | dp_is_latest                            | dp_created_at                                          | dp_replaced_at                                         |
|-------------------------------------------------------------------------|--------------------------------------|------------------------------------------|-------------------------------------------|-------------------------------------------|-------------------------------------------------------------|--------------------------------------------------------|--------------------------------------------------------|-----------------------------------------|-----------------------------------------|--------------------------------------------------------|--------------------------------------------------------|
| <span style='color: green;'>55b3ce1e-a181-4ff5-aaf0-c5b7f59e0104</span> | <span style='color: green;'>1</span> | <span style='color: green;'>Alice</span> | <span style='color: green;'>Meyer</span>  | <span style='color: green;'>Zurich</span> | <span style='color: green;'>alice.meyer@example.com</span>  | <span style='color: green;'>2026-01-05 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-06 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>c9ac4b58-7581-4210-8258-9250ace49913</span> | <span style='color: green;'>2</span> | <span style='color: green;'>Bob</span>   | <span style='color: green;'>Keller</span> | <span style='color: green;'>Bern</span>   | <span style='color: green;'>bob.keller@example.com</span>   | <span style='color: green;'>2026-01-05 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-06 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>60c3c335-2765-498a-bab4-bd589da85285</span> | <span style='color: green;'>3</span> | <span style='color: green;'>Clara</span> | <span style='color: green;'>Schmid</span> | <span style='color: green;'>Basel</span>  | <span style='color: green;'>clara.schmid@example.com</span> | <span style='color: green;'>2026-01-05 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-06 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |

_the following columns where excluded from the result: `record_hash, dp_load_timestamp, change_type`_

## Test Step 2
At 2026-01-01 00:00:00, insert the entity with `id=1` into the new partitions of the raw table and perform SCD2 merge.


**Raw Table `raw_person`**


|   id | first_name   | last_name   | city   | email                    | status   | dp_ts_from          | dp_loaded_at        |
|------|--------------|-------------|--------|--------------------------|----------|---------------------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-10 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-10 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-10 00:00:00 |



**Input to Merge**


| merge_key                            | dp_key                               |   id | first_name   | last_name   | city   | email                   | src_dp_ts_from      | load_ts             | status   | change_classification   | operation_type   | tgt_dp_ts_from      | tgt_dp_ts_to        | prev_dp_ts_from   | prev_dp_ts_to   | succ_dp_ts_from     | succ_dp_ts_to       |
|--------------------------------------|--------------------------------------|------|--------------|-------------|--------|-------------------------|---------------------|---------------------|----------|-------------------------|------------------|---------------------|---------------------|-------------------|-----------------|---------------------|---------------------|
| 55b3ce1e-a181-4ff5-aaf0-c5b7f59e0104 | 55b3ce1e-a181-4ff5-aaf0-c5b7f59e0104 |    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com | 2026-01-01 00:00:00 | 2026-01-10 00:00:00 | ACTIVE   | NEW_WITH_SUCC_SAME      | UPDATE_EXISTING  | 9999-12-31 23:59:59 | 9999-12-31 23:59:59 |                   |                 | 2026-01-05 00:00:00 | 9999-12-31 23:59:59 |



**Dimensional Table `dim_person`**


| dp_key                               |   id | first_name   | last_name   | city   | email                    | dp_ts_from                                              | dp_ts_to            | dp_is_active   | dp_is_latest   | dp_created_at       | dp_replaced_at                                          |
|--------------------------------------|------|--------------|-------------|--------|--------------------------|---------------------------------------------------------|---------------------|----------------|----------------|---------------------|---------------------------------------------------------|
| 55b3ce1e-a181-4ff5-aaf0-c5b7f59e0104 |    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | <span style='color: orange;'>2026-01-01 00:00:00</span> | 9999-12-31 23:59:59 | True           | True           | 2026-01-06 00:00:00 | <span style='color: orange;'>2026-01-11 00:00:00</span> |
| c9ac4b58-7581-4210-8258-9250ace49913 |    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | 2026-01-05 00:00:00                                     | 9999-12-31 23:59:59 | True           | True           | 2026-01-06 00:00:00 | 9999-12-31 23:59:59                                     |
| 60c3c335-2765-498a-bab4-bd589da85285 |    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | 2026-01-05 00:00:00                                     | 9999-12-31 23:59:59 | True           | True           | 2026-01-06 00:00:00 | 9999-12-31 23:59:59                                     |

_the following columns where excluded from the result: `record_hash, dp_load_timestamp, change_type`_

