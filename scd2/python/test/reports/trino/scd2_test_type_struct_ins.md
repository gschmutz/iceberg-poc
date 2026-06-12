# Testing Insert Operation with a column of type STRUCT

This test validates an INSERT operation of one new entity (with a 1st version) into a set of existing entities.


 * **Strategy:** `trino`
 * **Last Run:** `2026-06-09 16:44:52`
## Test Step 1
Insert 3 entities into raw table and perform initial SCD2 merge.


**Raw Table `raw_person`**


|   id | user_info                                                                                    | status   | dp_ts_from          | dp_loaded_at        |
|------|----------------------------------------------------------------------------------------------|----------|---------------------|---------------------|
|    1 | (first_name: 'Alice', last_name: 'Meyer', city: 'Zurich', email: 'alice.meyer@example.com')  | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    2 | (first_name: 'Bob', last_name: 'Keller', city: 'Bern', email: 'bob.keller@example.com')      | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    3 | (first_name: 'Clara', last_name: 'Schmid', city: 'Basel', email: 'clara.schmid@example.com') | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |



**Input to Merge**


| merge_record_id   | dp_record_id   |   id | user_info                                                                                    | dp_record_hash                                                   | dp_del_flag   | operation_type     | case_name   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   |
|-------------------|----------------|------|----------------------------------------------------------------------------------------------|------------------------------------------------------------------|---------------|--------------------|-------------|---------------------|---------------------|----------------|----------------|
|                   |                |    1 | (first_name: 'Alice', last_name: 'Meyer', city: 'Zurich', email: 'alice.meyer@example.com')  | 00B9A7122065F01BE7FD23C6FB962AEE6DE3B84D0BA50409DC26FC5A150FBDC8 | ACTIVE        | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           |
|                   |                |    2 | (first_name: 'Bob', last_name: 'Keller', city: 'Bern', email: 'bob.keller@example.com')      | D28A23C8422275E006FCF3D86AA51CF4E058FB495B8E48560FC9BF7BCC019B40 | ACTIVE        | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           |
|                   |                |    3 | (first_name: 'Clara', last_name: 'Schmid', city: 'Basel', email: 'clara.schmid@example.com') | 77C069EE2AA3730894A6E3319ADC455C203B6CC4D35B0B912C2FAADF3C687676 | ACTIVE        | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           |



**Dimensional Table `dim_person`**


| dp_record_id                                                            | id                                   | user_info                                                                                                                       | dp_ts_from                                             | dp_ts_to                                               | dp_is_active                            | dp_is_latest                            | dp_load_ts                                             | dp_replace_ts                                          |
|-------------------------------------------------------------------------|--------------------------------------|---------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------|--------------------------------------------------------|-----------------------------------------|-----------------------------------------|--------------------------------------------------------|--------------------------------------------------------|
| <span style='color: green;'>c837bcfa-b592-4640-985c-cbbf4f9c5de6</span> | <span style='color: green;'>1</span> | <span style='color: green;'>(first_name: 'Alice', last_name: 'Meyer', city: 'Zurich', email: 'alice.meyer@example.com')</span>  | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>3c1d9918-eac8-4097-94f8-86f86d54fb3f</span> | <span style='color: green;'>2</span> | <span style='color: green;'>(first_name: 'Bob', last_name: 'Keller', city: 'Bern', email: 'bob.keller@example.com')</span>      | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>e7581f8d-2572-4f6c-910c-800e2a788ce8</span> | <span style='color: green;'>3</span> | <span style='color: green;'>(first_name: 'Clara', last_name: 'Schmid', city: 'Basel', email: 'clara.schmid@example.com')</span> | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |

_the following columns where excluded from the result: `dp_record_hash`_

## Test Step 2
At 2026-01-05 00:00:00, insert the new entity with `id=10` into the new partition of the raw table and perform SCD2 merge.


**Raw Table `raw_person`**


|   id | user_info                                                                                    | status   | dp_ts_from          | dp_loaded_at        |
|------|----------------------------------------------------------------------------------------------|----------|---------------------|---------------------|
|    1 | (first_name: 'Alice', last_name: 'Meyer', city: 'Zurich', email: 'alice.meyer@example.com')  | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    2 | (first_name: 'Bob', last_name: 'Keller', city: 'Bern', email: 'bob.keller@example.com')      | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    3 | (first_name: 'Clara', last_name: 'Schmid', city: 'Basel', email: 'clara.schmid@example.com') | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    1 | (first_name: 'Alice', last_name: 'Meyer', city: 'Zurich', email: 'alice.meyer@example.com')  | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    2 | (first_name: 'Bob', last_name: 'Keller', city: 'Bern', email: 'bob.keller@example.com')      | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    3 | (first_name: 'Clara', last_name: 'Schmid', city: 'Basel', email: 'clara.schmid@example.com') | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|   10 | (first_name: 'Kevin', last_name: 'Loosli', city: 'Bern', email: 'kevin.loosli@example.com')  | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |



**Input to Merge**


| merge_record_id   | dp_record_id   |   id | user_info                                                                                   | dp_record_hash                                                   | dp_del_flag   | operation_type     | case_name   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   |
|-------------------|----------------|------|---------------------------------------------------------------------------------------------|------------------------------------------------------------------|---------------|--------------------|-------------|---------------------|---------------------|----------------|----------------|
|                   |                |   10 | (first_name: 'Kevin', last_name: 'Loosli', city: 'Bern', email: 'kevin.loosli@example.com') | F32E425B7483AA533A0DBD8DB41BBD3DEEDBD2FF6427D420A7130EC9B174787C | ACTIVE        | INSERT_NEW_VERSION | CASE_1      | 2026-01-05 00:00:00 | 9999-12-31 23:59:59 | True           | True           |



**Dimensional Table `dim_person`**


| dp_record_id                                                            | id                                    | user_info                                                                                                                      | dp_ts_from                                             | dp_ts_to                                               | dp_is_active                            | dp_is_latest                            | dp_load_ts                                             | dp_replace_ts                                          |
|-------------------------------------------------------------------------|---------------------------------------|--------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------|--------------------------------------------------------|-----------------------------------------|-----------------------------------------|--------------------------------------------------------|--------------------------------------------------------|
| c837bcfa-b592-4640-985c-cbbf4f9c5de6                                    | 1                                     | (first_name: 'Alice', last_name: 'Meyer', city: 'Zurich', email: 'alice.meyer@example.com')                                    | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                    | True                                    | True                                    | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                    |
| 3c1d9918-eac8-4097-94f8-86f86d54fb3f                                    | 2                                     | (first_name: 'Bob', last_name: 'Keller', city: 'Bern', email: 'bob.keller@example.com')                                        | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                    | True                                    | True                                    | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                    |
| e7581f8d-2572-4f6c-910c-800e2a788ce8                                    | 3                                     | (first_name: 'Clara', last_name: 'Schmid', city: 'Basel', email: 'clara.schmid@example.com')                                   | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                    | True                                    | True                                    | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                    |
| <span style='color: green;'>c8be285f-ec3c-454f-95f4-ec38bd029bb4</span> | <span style='color: green;'>10</span> | <span style='color: green;'>(first_name: 'Kevin', last_name: 'Loosli', city: 'Bern', email: 'kevin.loosli@example.com')</span> | <span style='color: green;'>2026-01-05 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-06 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |

_the following columns where excluded from the result: `dp_record_hash`_

