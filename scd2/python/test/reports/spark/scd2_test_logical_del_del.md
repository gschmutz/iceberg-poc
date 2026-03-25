# Testing Logical Delete Operation and that it stays consistent

This test validates a DELETE operation of a single entity. The delete is created by a logical delete in the raw table, i.e., the record status is set to INACTIVE. This test ensures that no further actions are taken with the same record in later partitions.
 * **Strategy:** `spark`
 * **Last Run:** `2026-03-25 14:08:34`
## Test Step 1
Insert 3 entities into raw table and perform initial SCD2 merge.


**Raw Table `raw_person`**


|   id | first_name   | last_name   | city   | email                    | status   | dp_ts_from          | dp_loaded_at        |
|------|--------------|-------------|--------|--------------------------|----------|---------------------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |



**Input to Merge**


| merge_key   | dp_key                               |   id | first_name   | last_name   | city   | email                    | record_hash                                                      | load_ts             | status   | operation_type     | case_name   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   |
|-------------|--------------------------------------|------|--------------|-------------|--------|--------------------------|------------------------------------------------------------------|---------------------|----------|--------------------|-------------|---------------------|---------------------|----------------|----------------|
|             | 01cd2d18-7695-448d-8dae-94b5b1928d6d |    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | 00B9A7122065F01BE7FD23C6FB962AEE6DE3B84D0BA50409DC26FC5A150FBDC8 | 2026-01-01 00:00:00 | ACTIVE   | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           |
|             | f5f33901-0e14-417b-a79a-f7fd7197d861 |    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | D28A23C8422275E006FCF3D86AA51CF4E058FB495B8E48560FC9BF7BCC019B40 | 2026-01-01 00:00:00 | ACTIVE   | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           |
|             | bf8df187-280d-4be2-b484-4491f4623028 |    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | 77C069EE2AA3730894A6E3319ADC455C203B6CC4D35B0B912C2FAADF3C687676 | 2026-01-01 00:00:00 | ACTIVE   | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           |



**Dimensional Table `dim_person`**


| dp_key                                                                  | id                                   | first_name                               | last_name                                 | city                                      | email                                                       | dp_ts_from                                             | dp_ts_to                                               | dp_is_active                            | dp_is_latest                            | dp_created_at                                          | dp_replaced_at                                         |
|-------------------------------------------------------------------------|--------------------------------------|------------------------------------------|-------------------------------------------|-------------------------------------------|-------------------------------------------------------------|--------------------------------------------------------|--------------------------------------------------------|-----------------------------------------|-----------------------------------------|--------------------------------------------------------|--------------------------------------------------------|
| <span style='color: green;'>2d97b58d-7c28-465f-8996-efcc7e2b95bb</span> | <span style='color: green;'>1</span> | <span style='color: green;'>Alice</span> | <span style='color: green;'>Meyer</span>  | <span style='color: green;'>Zurich</span> | <span style='color: green;'>alice.meyer@example.com</span>  | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>4c9e29cc-0df6-4645-a4c9-c75bcb02eb02</span> | <span style='color: green;'>2</span> | <span style='color: green;'>Bob</span>   | <span style='color: green;'>Keller</span> | <span style='color: green;'>Bern</span>   | <span style='color: green;'>bob.keller@example.com</span>   | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>3eee4b30-4f02-45bb-ab9e-b0bff0581d95</span> | <span style='color: green;'>3</span> | <span style='color: green;'>Clara</span> | <span style='color: green;'>Schmid</span> | <span style='color: green;'>Basel</span>  | <span style='color: green;'>clara.schmid@example.com</span> | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |

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


| merge_key                            | dp_key                               |   id | first_name   | last_name   | city   | email                    | record_hash                                                      | load_ts             | status   | operation_type   | case_name   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   |
|--------------------------------------|--------------------------------------|------|--------------|-------------|--------|--------------------------|------------------------------------------------------------------|---------------------|----------|------------------|-------------|---------------------|---------------------|----------------|----------------|
| 3eee4b30-4f02-45bb-ab9e-b0bff0581d95 | 3eee4b30-4f02-45bb-ab9e-b0bff0581d95 |    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | 77C069EE2AA3730894A6E3319ADC455C203B6CC4D35B0B912C2FAADF3C687676 | 2026-01-05 00:00:00 | INACTIVE | UPDATE_VERSION   | CASE_30     | 2026-01-01 00:00:00 | 2026-01-04 23:59:59 | False          | True           |



**Dimensional Table `dim_person`**


| dp_key                               |   id | first_name   | last_name   | city   | email                    | dp_ts_from          | dp_ts_to                                                | dp_is_active                              | dp_is_latest   | dp_created_at       | dp_replaced_at                                          |
|--------------------------------------|------|--------------|-------------|--------|--------------------------|---------------------|---------------------------------------------------------|-------------------------------------------|----------------|---------------------|---------------------------------------------------------|
| 2d97b58d-7c28-465f-8996-efcc7e2b95bb |    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | 2026-01-01 00:00:00 | 9999-12-31 23:59:59                                     | True                                      | True           | 2026-01-02 00:00:00 | 9999-12-31 23:59:59                                     |
| 4c9e29cc-0df6-4645-a4c9-c75bcb02eb02 |    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | 2026-01-01 00:00:00 | 9999-12-31 23:59:59                                     | True                                      | True           | 2026-01-02 00:00:00 | 9999-12-31 23:59:59                                     |
| 3eee4b30-4f02-45bb-ab9e-b0bff0581d95 |    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | 2026-01-01 00:00:00 | <span style='color: orange;'>2026-01-04 23:59:59</span> | <span style='color: orange;'>False</span> | True           | 2026-01-02 00:00:00 | <span style='color: orange;'>2026-01-06 00:00:00</span> |

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


| merge_key   | dp_key   | id   | first_name   | last_name   | city   | email   | record_hash   | load_ts   | status   | operation_type   | case_name   | dp_ts_from   | dp_ts_to   | dp_is_active   | dp_is_latest   |
|-------------|----------|------|--------------|-------------|--------|---------|---------------|-----------|----------|------------------|-------------|--------------|------------|----------------|----------------|



**Dimensional Table `dim_person`**


| dp_key                               |   id | first_name   | last_name   | city   | email                    | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   | dp_created_at       | dp_replaced_at      |
|--------------------------------------|------|--------------|-------------|--------|--------------------------|---------------------|---------------------|----------------|----------------|---------------------|---------------------|
| 2d97b58d-7c28-465f-8996-efcc7e2b95bb |    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           | 2026-01-02 00:00:00 | 9999-12-31 23:59:59 |
| 4c9e29cc-0df6-4645-a4c9-c75bcb02eb02 |    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           | 2026-01-02 00:00:00 | 9999-12-31 23:59:59 |
| 3eee4b30-4f02-45bb-ab9e-b0bff0581d95 |    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | 2026-01-01 00:00:00 | 2026-01-04 23:59:59 | False          | True           | 2026-01-02 00:00:00 | 2026-01-06 00:00:00 |

_the following columns where excluded from the result: `record_hash, dp_load_timestamp, change_type`_

