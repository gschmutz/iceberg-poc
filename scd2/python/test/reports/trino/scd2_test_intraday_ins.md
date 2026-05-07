# Testing Insert Operation

This test validates an INSERT operation of one new entity (with a 1st version) into a set of existing entities.


 * **Strategy:** `trino`
 * **Last Run:** `2026-05-07 13:30:37`
## Test Step 1
Insert 3 entities into raw table and perform initial SCD2 merge.


**Raw Table `raw_person`**


|   id | first_name   | last_name   | city   | email                    | status   | dp_ts_from          | dp_loaded_at        |
|------|--------------|-------------|--------|--------------------------|----------|---------------------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-01 09:00:00 | 2026-01-01 09:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-01 09:00:00 | 2026-01-01 09:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-01 09:00:00 | 2026-01-01 09:00:00 |



**Input to Merge**


| merge_record_id   | dp_record_id   |   id | first_name   | last_name   | city   | email                    | dp_record_hash                                                   | dp_del_flag   | operation_type     | case_name   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   |
|-------------------|----------------|------|--------------|-------------|--------|--------------------------|------------------------------------------------------------------|---------------|--------------------|-------------|---------------------|---------------------|----------------|----------------|
|                   |                |    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | 00B9A7122065F01BE7FD23C6FB962AEE6DE3B84D0BA50409DC26FC5A150FBDC8 | ACTIVE        | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 09:00:00 | 9999-12-31 23:59:59 | True           | True           |
|                   |                |    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | D28A23C8422275E006FCF3D86AA51CF4E058FB495B8E48560FC9BF7BCC019B40 | ACTIVE        | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 09:00:00 | 9999-12-31 23:59:59 | True           | True           |
|                   |                |    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | 77C069EE2AA3730894A6E3319ADC455C203B6CC4D35B0B912C2FAADF3C687676 | ACTIVE        | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 09:00:00 | 9999-12-31 23:59:59 | True           | True           |



**Dimensional Table `dim_person`**


| dp_record_id                                                            | id                                   | first_name                               | last_name                                 | city                                      | email                                                       | dp_ts_from                                             | dp_ts_to                                               | dp_is_active                            | dp_is_latest                            | dp_load_ts                                             | dp_replace_ts                                          |
|-------------------------------------------------------------------------|--------------------------------------|------------------------------------------|-------------------------------------------|-------------------------------------------|-------------------------------------------------------------|--------------------------------------------------------|--------------------------------------------------------|-----------------------------------------|-----------------------------------------|--------------------------------------------------------|--------------------------------------------------------|
| <span style='color: green;'>1f88c291-1df4-4bba-a732-ca1ef6c2986c</span> | <span style='color: green;'>1</span> | <span style='color: green;'>Alice</span> | <span style='color: green;'>Meyer</span>  | <span style='color: green;'>Zurich</span> | <span style='color: green;'>alice.meyer@example.com</span>  | <span style='color: green;'>2026-01-01 09:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-01 09:05:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>fb9a557f-cab1-4bd4-a6c4-71d89bbe43e4</span> | <span style='color: green;'>2</span> | <span style='color: green;'>Bob</span>   | <span style='color: green;'>Keller</span> | <span style='color: green;'>Bern</span>   | <span style='color: green;'>bob.keller@example.com</span>   | <span style='color: green;'>2026-01-01 09:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-01 09:05:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>3c64efdd-ce96-412e-bcc8-0f5ca585021f</span> | <span style='color: green;'>3</span> | <span style='color: green;'>Clara</span> | <span style='color: green;'>Schmid</span> | <span style='color: green;'>Basel</span>  | <span style='color: green;'>clara.schmid@example.com</span> | <span style='color: green;'>2026-01-01 09:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-01 09:05:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |

_the following columns where excluded from the result: `dp_record_hash`_

## Test Step 2
At 2026-01-01 09:10:00, insert the new entity with `id=10` into the new partition of the raw table and perform SCD2 merge.


**Raw Table `raw_person`**


|   id | first_name   | last_name   | city   | email                    | status   | dp_ts_from          | dp_loaded_at        |
|------|--------------|-------------|--------|--------------------------|----------|---------------------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-01 09:00:00 | 2026-01-01 09:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-01 09:00:00 | 2026-01-01 09:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-01 09:00:00 | 2026-01-01 09:00:00 |
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-01 09:10:00 | 2026-01-01 09:10:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-01 09:10:00 | 2026-01-01 09:10:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-01 09:10:00 | 2026-01-01 09:10:00 |
|   10 | Kevin        | Loosli      | Bern   | kevin.loosli@example.com | ACTIVE   | 2026-01-01 09:10:00 | 2026-01-01 09:10:00 |



**Input to Merge**


| merge_record_id   | dp_record_id   |   id | first_name   | last_name   | city   | email                    | dp_record_hash                                                   | dp_del_flag   | operation_type     | case_name   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   |
|-------------------|----------------|------|--------------|-------------|--------|--------------------------|------------------------------------------------------------------|---------------|--------------------|-------------|---------------------|---------------------|----------------|----------------|
|                   |                |   10 | Kevin        | Loosli      | Bern   | kevin.loosli@example.com | F32E425B7483AA533A0DBD8DB41BBD3DEEDBD2FF6427D420A7130EC9B174787C | ACTIVE        | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 09:10:00 | 9999-12-31 23:59:59 | True           | True           |



**Dimensional Table `dim_person`**


| dp_record_id                                                            | id                                    | first_name                               | last_name                                 | city                                    | email                                                       | dp_ts_from                                             | dp_ts_to                                               | dp_is_active                            | dp_is_latest                            | dp_load_ts                                             | dp_replace_ts                                          |
|-------------------------------------------------------------------------|---------------------------------------|------------------------------------------|-------------------------------------------|-----------------------------------------|-------------------------------------------------------------|--------------------------------------------------------|--------------------------------------------------------|-----------------------------------------|-----------------------------------------|--------------------------------------------------------|--------------------------------------------------------|
| 1f88c291-1df4-4bba-a732-ca1ef6c2986c                                    | 1                                     | Alice                                    | Meyer                                     | Zurich                                  | alice.meyer@example.com                                     | 2026-01-01 09:00:00                                    | 9999-12-31 23:59:59                                    | True                                    | True                                    | 2026-01-01 09:05:00                                    | 9999-12-31 23:59:59                                    |
| fb9a557f-cab1-4bd4-a6c4-71d89bbe43e4                                    | 2                                     | Bob                                      | Keller                                    | Bern                                    | bob.keller@example.com                                      | 2026-01-01 09:00:00                                    | 9999-12-31 23:59:59                                    | True                                    | True                                    | 2026-01-01 09:05:00                                    | 9999-12-31 23:59:59                                    |
| 3c64efdd-ce96-412e-bcc8-0f5ca585021f                                    | 3                                     | Clara                                    | Schmid                                    | Basel                                   | clara.schmid@example.com                                    | 2026-01-01 09:00:00                                    | 9999-12-31 23:59:59                                    | True                                    | True                                    | 2026-01-01 09:05:00                                    | 9999-12-31 23:59:59                                    |
| <span style='color: green;'>12184ba3-1cb2-4a0c-94d9-157bb9393738</span> | <span style='color: green;'>10</span> | <span style='color: green;'>Kevin</span> | <span style='color: green;'>Loosli</span> | <span style='color: green;'>Bern</span> | <span style='color: green;'>kevin.loosli@example.com</span> | <span style='color: green;'>2026-01-01 09:10:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-01 09:15:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |

_the following columns where excluded from the result: `dp_record_hash`_

