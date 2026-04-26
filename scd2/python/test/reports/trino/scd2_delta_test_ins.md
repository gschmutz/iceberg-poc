# Testing Insert Operation (Delta Mode for Source)

This test validates an INSERT operation of one new entity (with a 1st version) into a set of existing entities.


 * **Strategy:** `trino`
 * **Last Run:** `2026-04-26 21:03:31`
## Test Step 1
Insert 3 entities into raw table and perform initial SCD2 merge.


**Raw Table `raw_person`**


|   id | first_name   | last_name   | city   | email                    | status   | dp_ts_from          | dp_loaded_at        |
|------|--------------|-------------|--------|--------------------------|----------|---------------------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |



**Input to Merge**


| merge_record_id   | dp_record_id   |   id | first_name   | last_name   | city   | email                    | dp_record_hash                                                   | dp_ts               | status   | operation_type     | case_name   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   |
|-------------------|----------------|------|--------------|-------------|--------|--------------------------|------------------------------------------------------------------|---------------------|----------|--------------------|-------------|---------------------|---------------------|----------------|----------------|
|                   |                |    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | 00B9A7122065F01BE7FD23C6FB962AEE6DE3B84D0BA50409DC26FC5A150FBDC8 | 2026-01-01 00:00:00 | ACTIVE   | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           |
|                   |                |    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | D28A23C8422275E006FCF3D86AA51CF4E058FB495B8E48560FC9BF7BCC019B40 | 2026-01-01 00:00:00 | ACTIVE   | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           |
|                   |                |    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | 77C069EE2AA3730894A6E3319ADC455C203B6CC4D35B0B912C2FAADF3C687676 | 2026-01-01 00:00:00 | ACTIVE   | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           |



**Dimensional Table `dim_person`**


| dp_record_id                                                            | id                                   | first_name                               | last_name                                 | city                                      | email                                                       | dp_ts_from                                             | dp_ts_to                                               | dp_is_active                            | dp_is_latest                            | dp_created_at                                          | dp_replaced_at                                         |
|-------------------------------------------------------------------------|--------------------------------------|------------------------------------------|-------------------------------------------|-------------------------------------------|-------------------------------------------------------------|--------------------------------------------------------|--------------------------------------------------------|-----------------------------------------|-----------------------------------------|--------------------------------------------------------|--------------------------------------------------------|
| <span style='color: green;'>b651922a-d666-4092-aeb1-802df05edaad</span> | <span style='color: green;'>1</span> | <span style='color: green;'>Alice</span> | <span style='color: green;'>Meyer</span>  | <span style='color: green;'>Zurich</span> | <span style='color: green;'>alice.meyer@example.com</span>  | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>6154c762-b203-4249-bc1e-d74d1976ba09</span> | <span style='color: green;'>2</span> | <span style='color: green;'>Bob</span>   | <span style='color: green;'>Keller</span> | <span style='color: green;'>Bern</span>   | <span style='color: green;'>bob.keller@example.com</span>   | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>b4ff3dae-c9b8-4018-87f7-13215aadf72f</span> | <span style='color: green;'>3</span> | <span style='color: green;'>Clara</span> | <span style='color: green;'>Schmid</span> | <span style='color: green;'>Basel</span>  | <span style='color: green;'>clara.schmid@example.com</span> | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |

_the following columns where excluded from the result: `dp_record_hash, dp_load_timestamp, change_type`_

## Test Step 2
At 2026-01-05 00:00:00, insert the new entity with `id=10` into the new partition of the raw table and perform SCD2 merge.


**Raw Table `raw_person`**


|   id | first_name   | last_name   | city   | email                    | status   | dp_ts_from          | dp_loaded_at        |
|------|--------------|-------------|--------|--------------------------|----------|---------------------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|   10 | Kevin        | Loosli      | Bern   | kevin.loosli@example.com | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |



**Input to Merge**


| merge_record_id   | dp_record_id   |   id | first_name   | last_name   | city   | email                    | dp_record_hash                                                   | dp_ts               | status   | operation_type     | case_name   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   |
|-------------------|----------------|------|--------------|-------------|--------|--------------------------|------------------------------------------------------------------|---------------------|----------|--------------------|-------------|---------------------|---------------------|----------------|----------------|
|                   |                |   10 | Kevin        | Loosli      | Bern   | kevin.loosli@example.com | F32E425B7483AA533A0DBD8DB41BBD3DEEDBD2FF6427D420A7130EC9B174787C | 2026-01-05 00:00:00 | ACTIVE   | INSERT_NEW_VERSION | CASE_1      | 2026-01-05 00:00:00 | 9999-12-31 23:59:59 | True           | True           |



**Dimensional Table `dim_person`**


| dp_record_id                                                            | id                                    | first_name                               | last_name                                 | city                                    | email                                                       | dp_ts_from                                             | dp_ts_to                                               | dp_is_active                            | dp_is_latest                            | dp_created_at                                          | dp_replaced_at                                         |
|-------------------------------------------------------------------------|---------------------------------------|------------------------------------------|-------------------------------------------|-----------------------------------------|-------------------------------------------------------------|--------------------------------------------------------|--------------------------------------------------------|-----------------------------------------|-----------------------------------------|--------------------------------------------------------|--------------------------------------------------------|
| b651922a-d666-4092-aeb1-802df05edaad                                    | 1                                     | Alice                                    | Meyer                                     | Zurich                                  | alice.meyer@example.com                                     | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                    | True                                    | True                                    | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                    |
| 6154c762-b203-4249-bc1e-d74d1976ba09                                    | 2                                     | Bob                                      | Keller                                    | Bern                                    | bob.keller@example.com                                      | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                    | True                                    | True                                    | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                    |
| b4ff3dae-c9b8-4018-87f7-13215aadf72f                                    | 3                                     | Clara                                    | Schmid                                    | Basel                                   | clara.schmid@example.com                                    | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                    | True                                    | True                                    | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                    |
| <span style='color: green;'>e0a51d76-352b-4a5e-b8e1-b9d370bf08c5</span> | <span style='color: green;'>10</span> | <span style='color: green;'>Kevin</span> | <span style='color: green;'>Loosli</span> | <span style='color: green;'>Bern</span> | <span style='color: green;'>kevin.loosli@example.com</span> | <span style='color: green;'>2026-01-05 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-06 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |

_the following columns where excluded from the result: `dp_record_hash, dp_load_timestamp, change_type`_

