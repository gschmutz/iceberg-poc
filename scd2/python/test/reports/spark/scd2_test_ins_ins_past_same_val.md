# Testing Insert Operation followed by an Insert Operation in the past

This test validates an INSERT operation in the past of a version for an entity which already exists and with the same value.
 * **Strategy:** `spark`
 * **Last Run:** `2026-03-25 14:03:41`
## Test Step 1
At 2026-01-05 00:00:00, insert 3 entities into raw table and perform initial SCD2 merge.


**Raw Table `raw_person`**


|   id | first_name   | last_name   | city   | email                    | status   | dp_ts_from          | dp_loaded_at        |
|------|--------------|-------------|--------|--------------------------|----------|---------------------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |



**Input to Merge**


| merge_key   | dp_key                               |   id | first_name   | last_name   | city   | email                    | record_hash                                                      | load_ts             | status   | operation_type     | case_name   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   |
|-------------|--------------------------------------|------|--------------|-------------|--------|--------------------------|------------------------------------------------------------------|---------------------|----------|--------------------|-------------|---------------------|---------------------|----------------|----------------|
|             | f0417d91-29f9-4e07-b952-7c1ac1aa0912 |    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | 00B9A7122065F01BE7FD23C6FB962AEE6DE3B84D0BA50409DC26FC5A150FBDC8 | 2026-01-05 00:00:00 | ACTIVE   | INSERT_NEW_VERSION | CASE_1      | 2026-01-05 00:00:00 | 9999-12-31 23:59:59 | True           | True           |
|             | d8f90e95-7c03-4a81-a75d-eeabf2621ced |    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | D28A23C8422275E006FCF3D86AA51CF4E058FB495B8E48560FC9BF7BCC019B40 | 2026-01-05 00:00:00 | ACTIVE   | INSERT_NEW_VERSION | CASE_1      | 2026-01-05 00:00:00 | 9999-12-31 23:59:59 | True           | True           |
|             | ff6fea92-82ae-495f-a006-7c937fa9b73f |    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | 77C069EE2AA3730894A6E3319ADC455C203B6CC4D35B0B912C2FAADF3C687676 | 2026-01-05 00:00:00 | ACTIVE   | INSERT_NEW_VERSION | CASE_1      | 2026-01-05 00:00:00 | 9999-12-31 23:59:59 | True           | True           |



**Dimensional Table `dim_person`**


| dp_key                                                                  | id                                   | first_name                               | last_name                                 | city                                      | email                                                       | dp_ts_from                                             | dp_ts_to                                               | dp_is_active                            | dp_is_latest                            | dp_created_at                                          | dp_replaced_at                                         |
|-------------------------------------------------------------------------|--------------------------------------|------------------------------------------|-------------------------------------------|-------------------------------------------|-------------------------------------------------------------|--------------------------------------------------------|--------------------------------------------------------|-----------------------------------------|-----------------------------------------|--------------------------------------------------------|--------------------------------------------------------|
| <span style='color: green;'>09786057-e249-4b17-944d-e482533bec72</span> | <span style='color: green;'>1</span> | <span style='color: green;'>Alice</span> | <span style='color: green;'>Meyer</span>  | <span style='color: green;'>Zurich</span> | <span style='color: green;'>alice.meyer@example.com</span>  | <span style='color: green;'>2026-01-05 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-06 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>be005eb5-c7a0-4a1e-8804-daf1173ed1c8</span> | <span style='color: green;'>2</span> | <span style='color: green;'>Bob</span>   | <span style='color: green;'>Keller</span> | <span style='color: green;'>Bern</span>   | <span style='color: green;'>bob.keller@example.com</span>   | <span style='color: green;'>2026-01-05 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-06 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>9067fddb-8e71-4d44-a0bd-35a04445d186</span> | <span style='color: green;'>3</span> | <span style='color: green;'>Clara</span> | <span style='color: green;'>Schmid</span> | <span style='color: green;'>Basel</span>  | <span style='color: green;'>clara.schmid@example.com</span> | <span style='color: green;'>2026-01-05 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-06 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |

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


| merge_key                            | dp_key                               |   id | first_name   | last_name   | city   | email                   | record_hash                                                      | load_ts             | status   | operation_type   | case_name   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   |
|--------------------------------------|--------------------------------------|------|--------------|-------------|--------|-------------------------|------------------------------------------------------------------|---------------------|----------|------------------|-------------|---------------------|---------------------|----------------|----------------|
| 09786057-e249-4b17-944d-e482533bec72 | 09786057-e249-4b17-944d-e482533bec72 |    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com | 00B9A7122065F01BE7FD23C6FB962AEE6DE3B84D0BA50409DC26FC5A150FBDC8 | 2026-01-10 00:00:00 | ACTIVE   | UPDATE_VERSION   | CASE_18     | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 |                |                |



**Dimensional Table `dim_person`**


| dp_key                               |   id | first_name   | last_name   | city   | email                    | dp_ts_from                                              | dp_ts_to            | dp_is_active   | dp_is_latest   | dp_created_at       | dp_replaced_at                                          |
|--------------------------------------|------|--------------|-------------|--------|--------------------------|---------------------------------------------------------|---------------------|----------------|----------------|---------------------|---------------------------------------------------------|
| 09786057-e249-4b17-944d-e482533bec72 |    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | <span style='color: orange;'>2026-01-01 00:00:00</span> | 9999-12-31 23:59:59 | True           | True           | 2026-01-06 00:00:00 | <span style='color: orange;'>2026-01-11 00:00:00</span> |
| be005eb5-c7a0-4a1e-8804-daf1173ed1c8 |    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | 2026-01-05 00:00:00                                     | 9999-12-31 23:59:59 | True           | True           | 2026-01-06 00:00:00 | 9999-12-31 23:59:59                                     |
| 9067fddb-8e71-4d44-a0bd-35a04445d186 |    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | 2026-01-05 00:00:00                                     | 9999-12-31 23:59:59 | True           | True           | 2026-01-06 00:00:00 | 9999-12-31 23:59:59                                     |

_the following columns where excluded from the result: `record_hash, dp_load_timestamp, change_type`_

