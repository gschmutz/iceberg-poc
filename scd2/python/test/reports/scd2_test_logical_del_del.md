# Testing Logical Delete Operation and that it stays consistent

This test validates a DELETE operation of a single entity. The delete is created by a logical delete in the raw table, i.e., the record status is set to INACTIVE. This test ensures that no further actions are taken with the same record in later partitions.
## Test Step 1
Insert 3 entities into raw table and perform initial SCD2 merge.


**Raw Table `raw_person`**


|   id | first_name   | last_name   | city   | email                    | status   | dp_ts_from          | dp_loaded_at        |
|------|--------------|-------------|--------|--------------------------|----------|---------------------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |



**Input to Merge**


| merge_key   | dp_key   |   id | first_name   | last_name   | city   | email                    | src_dp_ts_from      | load_ts             | status   | change_classification   | operation_type   | tgt_dp_ts_from      | tgt_dp_ts_to        | prev_dp_ts_from   | prev_dp_ts_to   | succ_dp_ts_from   | succ_dp_ts_to   |
|-------------|----------|------|--------------|-------------|--------|--------------------------|---------------------|---------------------|----------|-------------------------|------------------|---------------------|---------------------|-------------------|-----------------|-------------------|-----------------|
|             |          |    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 | ACTIVE   | NEW                     | UPDATE_EXISTING  | 9999-12-31 23:59:59 | 9999-12-31 23:59:59 |                   |                 |                   |                 |
|             |          |    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 | ACTIVE   | NEW                     | UPDATE_EXISTING  | 9999-12-31 23:59:59 | 9999-12-31 23:59:59 |                   |                 |                   |                 |
|             |          |    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 | ACTIVE   | NEW                     | UPDATE_EXISTING  | 9999-12-31 23:59:59 | 9999-12-31 23:59:59 |                   |                 |                   |                 |



**Dimensional Table `dim_person`**


| dp_key                                                                  | id                                   | first_name                               | last_name                                 | city                                      | email                                                       | dp_ts_from                                             | dp_ts_to                                               | dp_is_active                            | dp_is_latest                            | dp_created_at                                          | dp_replaced_at                                         |
|-------------------------------------------------------------------------|--------------------------------------|------------------------------------------|-------------------------------------------|-------------------------------------------|-------------------------------------------------------------|--------------------------------------------------------|--------------------------------------------------------|-----------------------------------------|-----------------------------------------|--------------------------------------------------------|--------------------------------------------------------|
| <span style='color: green;'>bf2b021b-f620-45b3-b4b0-09c8860b05c2</span> | <span style='color: green;'>1</span> | <span style='color: green;'>Alice</span> | <span style='color: green;'>Meyer</span>  | <span style='color: green;'>Zurich</span> | <span style='color: green;'>alice.meyer@example.com</span>  | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>fece4613-0f3d-4e14-b3ed-ad0f01a9be50</span> | <span style='color: green;'>2</span> | <span style='color: green;'>Bob</span>   | <span style='color: green;'>Keller</span> | <span style='color: green;'>Bern</span>   | <span style='color: green;'>bob.keller@example.com</span>   | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>637f90ae-8352-4da3-86de-7eace1625506</span> | <span style='color: green;'>3</span> | <span style='color: green;'>Clara</span> | <span style='color: green;'>Schmid</span> | <span style='color: green;'>Basel</span>  | <span style='color: green;'>clara.schmid@example.com</span> | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |

_the following columns where excluded from the result: `record_hash, dp_load_timestamp, change_type`_

## Test Step 2
At 2026-01-05 00:00:00, delete entity with `id=3` in raw table by setting it to INACTIVE (logical delete) and perform SCD2 merge.


**Raw Table `raw_person`**


|   id | first_name   | last_name   | city   | email                    | status   | dp_ts_from          | dp_loaded_at        |
|------|--------------|-------------|--------|--------------------------|----------|---------------------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | INACTIVE | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |



**Input to Merge**


| merge_key                            | dp_key                               |   id | first_name   | last_name   | city   | email                    | src_dp_ts_from      | load_ts             | status   | change_classification   | operation_type   | tgt_dp_ts_from      | tgt_dp_ts_to        | prev_dp_ts_from   | prev_dp_ts_to   | succ_dp_ts_from   | succ_dp_ts_to   |
|--------------------------------------|--------------------------------------|------|--------------|-------------|--------|--------------------------|---------------------|---------------------|----------|-------------------------|------------------|---------------------|---------------------|-------------------|-----------------|-------------------|-----------------|
| 637f90ae-8352-4da3-86de-7eace1625506 | 637f90ae-8352-4da3-86de-7eace1625506 |    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 | INACTIVE | DELETED                 | UPDATE_EXISTING  | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 |                   |                 |                   |                 |



**Dimensional Table `dim_person`**


| dp_key                               |   id | first_name   | last_name   | city   | email                    | dp_ts_from          | dp_ts_to                                                | dp_is_active                              | dp_is_latest   | dp_created_at       | dp_replaced_at                                          |
|--------------------------------------|------|--------------|-------------|--------|--------------------------|---------------------|---------------------------------------------------------|-------------------------------------------|----------------|---------------------|---------------------------------------------------------|
| bf2b021b-f620-45b3-b4b0-09c8860b05c2 |    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | 2026-01-01 00:00:00 | 9999-12-31 23:59:59                                     | True                                      | True           | 2026-01-02 00:00:00 | 9999-12-31 23:59:59                                     |
| fece4613-0f3d-4e14-b3ed-ad0f01a9be50 |    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | 2026-01-01 00:00:00 | 9999-12-31 23:59:59                                     | True                                      | True           | 2026-01-02 00:00:00 | 9999-12-31 23:59:59                                     |
| 637f90ae-8352-4da3-86de-7eace1625506 |    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | 2026-01-01 00:00:00 | <span style='color: orange;'>2026-01-04 23:59:59</span> | <span style='color: orange;'>False</span> | True           | 2026-01-02 00:00:00 | <span style='color: orange;'>2026-01-06 00:00:00</span> |

_the following columns where excluded from the result: `record_hash, dp_load_timestamp, change_type`_

## Test Step 3
At 2026-01-10 00:00:00, still have the entity with `id=3` in raw table as INACTIVE (logical delete) and perform SCD2 merge. Nothing should change for the record with `id=3` in dimension table.


**Raw Table `raw_person`**


|   id | first_name   | last_name   | city   | email                    | status   | dp_ts_from          | dp_loaded_at        |
|------|--------------|-------------|--------|--------------------------|----------|---------------------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | INACTIVE | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-10 00:00:00 | 2026-01-10 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-10 00:00:00 | 2026-01-10 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | INACTIVE | 2026-01-10 00:00:00 | 2026-01-10 00:00:00 |



**Input to Merge**


| merge_key   | dp_key   | id   | first_name   | last_name   | city   | email   | src_dp_ts_from   | load_ts   | status   | change_classification   | operation_type   | tgt_dp_ts_from   | tgt_dp_ts_to   | prev_dp_ts_from   | prev_dp_ts_to   | succ_dp_ts_from   | succ_dp_ts_to   |
|-------------|----------|------|--------------|-------------|--------|---------|------------------|-----------|----------|-------------------------|------------------|------------------|----------------|-------------------|-----------------|-------------------|-----------------|



**Dimensional Table `dim_person`**


| dp_key                               |   id | first_name   | last_name   | city   | email                    | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   | dp_created_at       | dp_replaced_at      |
|--------------------------------------|------|--------------|-------------|--------|--------------------------|---------------------|---------------------|----------------|----------------|---------------------|---------------------|
| bf2b021b-f620-45b3-b4b0-09c8860b05c2 |    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           | 2026-01-02 00:00:00 | 9999-12-31 23:59:59 |
| fece4613-0f3d-4e14-b3ed-ad0f01a9be50 |    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           | 2026-01-02 00:00:00 | 9999-12-31 23:59:59 |
| 637f90ae-8352-4da3-86de-7eace1625506 |    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | 2026-01-01 00:00:00 | 2026-01-04 23:59:59 | False          | True           | 2026-01-02 00:00:00 | 2026-01-06 00:00:00 |

_the following columns where excluded from the result: `record_hash, dp_load_timestamp, change_type`_

