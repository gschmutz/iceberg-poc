# Testing Insert Operation

This test validates an INSERT operation of one new entity (with a 1st version) into a set of existing entities.


 * **Strategy:** `spark`
 * **Last Run:** `2026-05-29 21:32:36`
## Test Step 1
Insert 3 entities into raw table and perform initial SCD2 merge.


**Raw Table `raw_person`**


|   id | user_info                                                                                            | status   | dp_ts_from          | dp_loaded_at        |
|------|------------------------------------------------------------------------------------------------------|----------|---------------------|---------------------|
|    1 | {'first_name': 'Alice', 'last_name': 'Meyer', 'city': 'Zurich', 'email': 'alice.meyer@example.com'}  | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    2 | {'first_name': 'Bob', 'last_name': 'Keller', 'city': 'Bern', 'email': 'bob.keller@example.com'}      | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    3 | {'first_name': 'Clara', 'last_name': 'Schmid', 'city': 'Basel', 'email': 'clara.schmid@example.com'} | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |



**Input to Merge**


| merge_record_id   | dp_record_id                         |   id | user_info                                                                                            | dp_record_hash                                                   | dp_del_flag   | operation_type     | case_name   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   |
|-------------------|--------------------------------------|------|------------------------------------------------------------------------------------------------------|------------------------------------------------------------------|---------------|--------------------|-------------|---------------------|---------------------|----------------|----------------|
|                   | 5b5bac2f-9573-14ea-23df-ca781b0e2fbb |    1 | {'first_name': 'Alice', 'last_name': 'Meyer', 'city': 'Zurich', 'email': 'alice.meyer@example.com'}  | F379F42F805729CFC146CDA0164C5E898C7AD060C07D291F692DA97B3823A7D9 | ACTIVE        | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           |
|                   | e683c742-63f3-6271-64ed-b9a8b4c5cb32 |    2 | {'first_name': 'Bob', 'last_name': 'Keller', 'city': 'Bern', 'email': 'bob.keller@example.com'}      | 228993688DBB10E60E1CAC8F1D0AA141FDEADB2FC38B9C8A3483DC61655F6B3B | ACTIVE        | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           |
|                   | af503a22-602c-adb7-cf8d-ca124df6fcf8 |    3 | {'first_name': 'Clara', 'last_name': 'Schmid', 'city': 'Basel', 'email': 'clara.schmid@example.com'} | 42E013F11875C7C485E0080FFD38BE6C27A978342DE9346F1D8CD189BF0894E0 | ACTIVE        | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           |



**Dimensional Table `dim_person`**


| dp_record_id                                                            | id                                   | user_info                                                                                                                               | dp_ts_from                                             | dp_ts_to                                               | dp_is_active                            | dp_is_latest                            | dp_load_ts                                             | dp_replace_ts                                          |
|-------------------------------------------------------------------------|--------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------|--------------------------------------------------------|-----------------------------------------|-----------------------------------------|--------------------------------------------------------|--------------------------------------------------------|
| <span style='color: green;'>5b5bac2f-9573-14ea-23df-ca781b0e2fbb</span> | <span style='color: green;'>1</span> | <span style='color: green;'>{'first_name': 'Alice', 'last_name': 'Meyer', 'city': 'Zurich', 'email': 'alice.meyer@example.com'}</span>  | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>e683c742-63f3-6271-64ed-b9a8b4c5cb32</span> | <span style='color: green;'>2</span> | <span style='color: green;'>{'first_name': 'Bob', 'last_name': 'Keller', 'city': 'Bern', 'email': 'bob.keller@example.com'}</span>      | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>af503a22-602c-adb7-cf8d-ca124df6fcf8</span> | <span style='color: green;'>3</span> | <span style='color: green;'>{'first_name': 'Clara', 'last_name': 'Schmid', 'city': 'Basel', 'email': 'clara.schmid@example.com'}</span> | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |

_the following columns where excluded from the result: `dp_record_hash`_

## Test Step 2
At 2026-01-05 00:00:00, insert the new entity with `id=10` into the new partition of the raw table and perform SCD2 merge.


**Raw Table `raw_person`**


|   id | user_info                                                                                            | status   | dp_ts_from          | dp_loaded_at        |
|------|------------------------------------------------------------------------------------------------------|----------|---------------------|---------------------|
|    1 | {'first_name': 'Alice', 'last_name': 'Meyer', 'city': 'Zurich', 'email': 'alice.meyer@example.com'}  | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    2 | {'first_name': 'Bob', 'last_name': 'Keller', 'city': 'Bern', 'email': 'bob.keller@example.com'}      | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    3 | {'first_name': 'Clara', 'last_name': 'Schmid', 'city': 'Basel', 'email': 'clara.schmid@example.com'} | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    1 | {'first_name': 'Alice', 'last_name': 'Meyer', 'city': 'Zurich', 'email': 'alice.meyer@example.com'}  | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    2 | {'first_name': 'Bob', 'last_name': 'Keller', 'city': 'Bern', 'email': 'bob.keller@example.com'}      | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    3 | {'first_name': 'Clara', 'last_name': 'Schmid', 'city': 'Basel', 'email': 'clara.schmid@example.com'} | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|   10 | {'first_name': 'Kevin', 'last_name': 'Loosli', 'city': 'Bern', 'email': 'kevin.loosli@example.com'}  | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |



**Input to Merge**


| merge_record_id   | dp_record_id                         |   id | user_info                                                                                           | dp_record_hash                                                   | dp_del_flag   | operation_type     | case_name   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   |
|-------------------|--------------------------------------|------|-----------------------------------------------------------------------------------------------------|------------------------------------------------------------------|---------------|--------------------|-------------|---------------------|---------------------|----------------|----------------|
|                   | e4059a02-b48e-fc90-b4e8-42b7ad0de301 |   10 | {'first_name': 'Kevin', 'last_name': 'Loosli', 'city': 'Bern', 'email': 'kevin.loosli@example.com'} | BBDC7F9E78A87D2F0CCE5106F1735616B93F60FA3C442BFC2A792BFAA11F3CC1 | ACTIVE        | INSERT_NEW_VERSION | CASE_1      | 2026-01-05 00:00:00 | 9999-12-31 23:59:59 | True           | True           |



**Dimensional Table `dim_person`**


| dp_record_id                                                            | id                                    | user_info                                                                                                                              | dp_ts_from                                             | dp_ts_to                                               | dp_is_active                            | dp_is_latest                            | dp_load_ts                                             | dp_replace_ts                                          |
|-------------------------------------------------------------------------|---------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------|--------------------------------------------------------|-----------------------------------------|-----------------------------------------|--------------------------------------------------------|--------------------------------------------------------|
| 5b5bac2f-9573-14ea-23df-ca781b0e2fbb                                    | 1                                     | {'first_name': 'Alice', 'last_name': 'Meyer', 'city': 'Zurich', 'email': 'alice.meyer@example.com'}                                    | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                    | True                                    | True                                    | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                    |
| e683c742-63f3-6271-64ed-b9a8b4c5cb32                                    | 2                                     | {'first_name': 'Bob', 'last_name': 'Keller', 'city': 'Bern', 'email': 'bob.keller@example.com'}                                        | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                    | True                                    | True                                    | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                    |
| af503a22-602c-adb7-cf8d-ca124df6fcf8                                    | 3                                     | {'first_name': 'Clara', 'last_name': 'Schmid', 'city': 'Basel', 'email': 'clara.schmid@example.com'}                                   | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                    | True                                    | True                                    | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                    |
| <span style='color: green;'>e4059a02-b48e-fc90-b4e8-42b7ad0de301</span> | <span style='color: green;'>10</span> | <span style='color: green;'>{'first_name': 'Kevin', 'last_name': 'Loosli', 'city': 'Bern', 'email': 'kevin.loosli@example.com'}</span> | <span style='color: green;'>2026-01-05 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-06 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |

_the following columns where excluded from the result: `dp_record_hash`_

