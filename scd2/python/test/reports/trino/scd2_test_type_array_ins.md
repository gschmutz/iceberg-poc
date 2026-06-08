# Testing Insert Operation with a column of type ARRAY

This test validates an INSERT operation of one new entity (with a 1st version) into a set of existing entities.


 * **Strategy:** `trino`
 * **Last Run:** `2026-06-08 18:39:33`
## Test Step 1
Insert 3 entities into raw table and perform initial SCD2 merge.


**Raw Table `raw_person`**


|   id | user_info                                                | status   | dp_ts_from          | dp_loaded_at        |
|------|----------------------------------------------------------|----------|---------------------|---------------------|
|    1 | ['Alice', 'Meyer', 'Zurich', 'alice.meyer@example.com']  | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    2 | ['Bob', 'Keller', 'Bern', 'bob.keller@example.com']      | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    3 | ['Clara', 'Schmid', 'Basel', 'clara.schmid@example.com'] | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |



**Input to Merge**


| merge_record_id   | dp_record_id   |   id | user_info                                                | dp_record_hash                                                   | dp_del_flag   | operation_type     | case_name   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   |
|-------------------|----------------|------|----------------------------------------------------------|------------------------------------------------------------------|---------------|--------------------|-------------|---------------------|---------------------|----------------|----------------|
|                   |                |    1 | ['Alice', 'Meyer', 'Zurich', 'alice.meyer@example.com']  | FCA31A8773CBA1D201E0FD46712E14B4730F9D768A1023645BFD80A6005A73D7 | ACTIVE        | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           |
|                   |                |    2 | ['Bob', 'Keller', 'Bern', 'bob.keller@example.com']      | 546522DC8B793A74513486D16BE4C0A3A448BA8E65F6BE16E500D90FCB01DB66 | ACTIVE        | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           |
|                   |                |    3 | ['Clara', 'Schmid', 'Basel', 'clara.schmid@example.com'] | 769F215ED61370AF86BE40AC7A29DFC3F067B55636B6918AD886C12619646091 | ACTIVE        | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           |



**Dimensional Table `dim_person`**


| dp_record_id                                                            | id                                   | user_info                                                                                   | dp_ts_from                                             | dp_ts_to                                               | dp_is_active                            | dp_is_latest                            | dp_load_ts                                             | dp_replace_ts                                          |
|-------------------------------------------------------------------------|--------------------------------------|---------------------------------------------------------------------------------------------|--------------------------------------------------------|--------------------------------------------------------|-----------------------------------------|-----------------------------------------|--------------------------------------------------------|--------------------------------------------------------|
| <span style='color: green;'>88b467f5-1670-4b57-b45b-407c3e67d4c9</span> | <span style='color: green;'>1</span> | <span style='color: green;'>['Alice', 'Meyer', 'Zurich', 'alice.meyer@example.com']</span>  | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>e8551887-1177-45ce-a770-1862844e3736</span> | <span style='color: green;'>2</span> | <span style='color: green;'>['Bob', 'Keller', 'Bern', 'bob.keller@example.com']</span>      | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>7df443fd-294b-4bf8-b7c9-b29e43de9ba0</span> | <span style='color: green;'>3</span> | <span style='color: green;'>['Clara', 'Schmid', 'Basel', 'clara.schmid@example.com']</span> | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |

_the following columns where excluded from the result: `dp_record_hash`_

## Test Step 2
At 2026-01-05 00:00:00, insert the new entity with `id=10` into the new partition of the raw table and perform SCD2 merge.


**Raw Table `raw_person`**


|   id | user_info                                                | status   | dp_ts_from          | dp_loaded_at        |
|------|----------------------------------------------------------|----------|---------------------|---------------------|
|    1 | ['Alice', 'Meyer', 'Zurich', 'alice.meyer@example.com']  | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    2 | ['Bob', 'Keller', 'Bern', 'bob.keller@example.com']      | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    3 | ['Clara', 'Schmid', 'Basel', 'clara.schmid@example.com'] | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    1 | ['Alice', 'Meyer', 'Zurich', 'alice.meyer@example.com']  | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    2 | ['Bob', 'Keller', 'Bern', 'bob.keller@example.com']      | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    3 | ['Clara', 'Schmid', 'Basel', 'clara.schmid@example.com'] | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|   10 | ['Kevin', 'Loosli', 'Bern', 'kevin.loosli@example.com']  | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |



**Input to Merge**


| merge_record_id   | dp_record_id   |   id | user_info                                               | dp_record_hash                                                   | dp_del_flag   | operation_type     | case_name   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   |
|-------------------|----------------|------|---------------------------------------------------------|------------------------------------------------------------------|---------------|--------------------|-------------|---------------------|---------------------|----------------|----------------|
|                   |                |   10 | ['Kevin', 'Loosli', 'Bern', 'kevin.loosli@example.com'] | 7F9D750B25D56FBC364C4D224E3999EDD183DC8F3AF153660450F165E001239C | ACTIVE        | INSERT_NEW_VERSION | CASE_1      | 2026-01-05 00:00:00 | 9999-12-31 23:59:59 | True           | True           |



**Dimensional Table `dim_person`**


| dp_record_id                                                            | id                                    | user_info                                                                                  | dp_ts_from                                             | dp_ts_to                                               | dp_is_active                            | dp_is_latest                            | dp_load_ts                                             | dp_replace_ts                                          |
|-------------------------------------------------------------------------|---------------------------------------|--------------------------------------------------------------------------------------------|--------------------------------------------------------|--------------------------------------------------------|-----------------------------------------|-----------------------------------------|--------------------------------------------------------|--------------------------------------------------------|
| 88b467f5-1670-4b57-b45b-407c3e67d4c9                                    | 1                                     | ['Alice', 'Meyer', 'Zurich', 'alice.meyer@example.com']                                    | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                    | True                                    | True                                    | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                    |
| e8551887-1177-45ce-a770-1862844e3736                                    | 2                                     | ['Bob', 'Keller', 'Bern', 'bob.keller@example.com']                                        | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                    | True                                    | True                                    | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                    |
| 7df443fd-294b-4bf8-b7c9-b29e43de9ba0                                    | 3                                     | ['Clara', 'Schmid', 'Basel', 'clara.schmid@example.com']                                   | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                    | True                                    | True                                    | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                    |
| <span style='color: green;'>71dffbe2-647a-4a61-81f4-86d33c36d054</span> | <span style='color: green;'>10</span> | <span style='color: green;'>['Kevin', 'Loosli', 'Bern', 'kevin.loosli@example.com']</span> | <span style='color: green;'>2026-01-05 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-06 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |

_the following columns where excluded from the result: `dp_record_hash`_

