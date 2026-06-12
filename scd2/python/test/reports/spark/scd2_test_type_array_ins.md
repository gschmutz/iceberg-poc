# Testing Insert Operation with a column of type ARRAY

This test validates an INSERT operation of one new entity (with a 1st version) into a set of existing entities.


 * **Strategy:** `spark`
 * **Last Run:** `2026-06-10 11:59:13`
## Test Step 1
Insert 3 entities into raw table and perform initial SCD2 merge.


**Raw Table `raw_person`**


|   id | user_info                                             | status   | dp_ts_from          | dp_loaded_at        |
|------|-------------------------------------------------------|----------|---------------------|---------------------|
|    1 | ['Alice' 'Meyer' 'Zurich' 'alice.meyer@example.com']  | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    2 | ['Bob' 'Keller' 'Bern' 'bob.keller@example.com']      | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    3 | ['Clara' 'Schmid' 'Basel' 'clara.schmid@example.com'] | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |



**Input to Merge**


|   merge_record_id | dp_record_id                         |   id | user_info                                             | dp_record_hash                                                   | dp_del_flag   | operation_type     | case_name   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   |
|-------------------|--------------------------------------|------|-------------------------------------------------------|------------------------------------------------------------------|---------------|--------------------|-------------|---------------------|---------------------|----------------|----------------|
|               nan | 5b5bac2f-9573-14ea-23df-ca781b0e2fbb |    1 | ['Alice' 'Meyer' 'Zurich' 'alice.meyer@example.com']  | FCA31A8773CBA1D201E0FD46712E14B4730F9D768A1023645BFD80A6005A73D7 | ACTIVE        | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           |
|               nan | e683c742-63f3-6271-64ed-b9a8b4c5cb32 |    2 | ['Bob' 'Keller' 'Bern' 'bob.keller@example.com']      | 546522DC8B793A74513486D16BE4C0A3A448BA8E65F6BE16E500D90FCB01DB66 | ACTIVE        | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           |
|               nan | af503a22-602c-adb7-cf8d-ca124df6fcf8 |    3 | ['Clara' 'Schmid' 'Basel' 'clara.schmid@example.com'] | 769F215ED61370AF86BE40AC7A29DFC3F067B55636B6918AD886C12619646091 | ACTIVE        | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           |



**Dimensional Table `dim_person`**


| dp_record_id                                                            | id                                   | user_info                                                                                | dp_ts_from                                             | dp_ts_to                                               | dp_is_active                            | dp_is_latest                            | dp_load_ts                                             | dp_replace_ts                                          |
|-------------------------------------------------------------------------|--------------------------------------|------------------------------------------------------------------------------------------|--------------------------------------------------------|--------------------------------------------------------|-----------------------------------------|-----------------------------------------|--------------------------------------------------------|--------------------------------------------------------|
| <span style='color: green;'>5b5bac2f-9573-14ea-23df-ca781b0e2fbb</span> | <span style='color: green;'>1</span> | <span style='color: green;'>['Alice' 'Meyer' 'Zurich' 'alice.meyer@example.com']</span>  | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>e683c742-63f3-6271-64ed-b9a8b4c5cb32</span> | <span style='color: green;'>2</span> | <span style='color: green;'>['Bob' 'Keller' 'Bern' 'bob.keller@example.com']</span>      | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>af503a22-602c-adb7-cf8d-ca124df6fcf8</span> | <span style='color: green;'>3</span> | <span style='color: green;'>['Clara' 'Schmid' 'Basel' 'clara.schmid@example.com']</span> | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |

_the following columns where excluded from the result: `dp_record_hash`_

## Test Step 2
At 2026-01-05 00:00:00, insert the new entity with `id=10` into the new partition of the raw table and perform SCD2 merge.


**Raw Table `raw_person`**


|   id | user_info                                             | status   | dp_ts_from          | dp_loaded_at        |
|------|-------------------------------------------------------|----------|---------------------|---------------------|
|    1 | ['Alice' 'Meyer' 'Zurich' 'alice.meyer@example.com']  | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    2 | ['Bob' 'Keller' 'Bern' 'bob.keller@example.com']      | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    3 | ['Clara' 'Schmid' 'Basel' 'clara.schmid@example.com'] | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    1 | ['Alice' 'Meyer' 'Zurich' 'alice.meyer@example.com']  | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    2 | ['Bob' 'Keller' 'Bern' 'bob.keller@example.com']      | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    3 | ['Clara' 'Schmid' 'Basel' 'clara.schmid@example.com'] | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|   10 | ['Kevin' 'Loosli' 'Bern' 'kevin.loosli@example.com']  | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |



**Input to Merge**


|   merge_record_id | dp_record_id                         |   id | user_info                                            | dp_record_hash                                                   | dp_del_flag   | operation_type     | case_name   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   |
|-------------------|--------------------------------------|------|------------------------------------------------------|------------------------------------------------------------------|---------------|--------------------|-------------|---------------------|---------------------|----------------|----------------|
|               nan | e4059a02-b48e-fc90-b4e8-42b7ad0de301 |   10 | ['Kevin' 'Loosli' 'Bern' 'kevin.loosli@example.com'] | 7F9D750B25D56FBC364C4D224E3999EDD183DC8F3AF153660450F165E001239C | ACTIVE        | INSERT_NEW_VERSION | CASE_1      | 2026-01-05 00:00:00 | 9999-12-31 23:59:59 | True           | True           |



**Dimensional Table `dim_person`**


| dp_record_id                                                            | id                                    | user_info                                                                               | dp_ts_from                                             | dp_ts_to                                               | dp_is_active                            | dp_is_latest                            | dp_load_ts                                             | dp_replace_ts                                          |
|-------------------------------------------------------------------------|---------------------------------------|-----------------------------------------------------------------------------------------|--------------------------------------------------------|--------------------------------------------------------|-----------------------------------------|-----------------------------------------|--------------------------------------------------------|--------------------------------------------------------|
| 5b5bac2f-9573-14ea-23df-ca781b0e2fbb                                    | 1                                     | ['Alice' 'Meyer' 'Zurich' 'alice.meyer@example.com']                                    | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                    | True                                    | True                                    | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                    |
| e683c742-63f3-6271-64ed-b9a8b4c5cb32                                    | 2                                     | ['Bob' 'Keller' 'Bern' 'bob.keller@example.com']                                        | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                    | True                                    | True                                    | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                    |
| af503a22-602c-adb7-cf8d-ca124df6fcf8                                    | 3                                     | ['Clara' 'Schmid' 'Basel' 'clara.schmid@example.com']                                   | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                    | True                                    | True                                    | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                    |
| <span style='color: green;'>e4059a02-b48e-fc90-b4e8-42b7ad0de301</span> | <span style='color: green;'>10</span> | <span style='color: green;'>['Kevin' 'Loosli' 'Bern' 'kevin.loosli@example.com']</span> | <span style='color: green;'>2026-01-05 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-06 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |

_the following columns where excluded from the result: `dp_record_hash`_

