# Testing Insert Operation

This test validates an INSERT operation of one new entity (with a 1st version) into a set of existing entities.


 * **Strategy:** `trino`
 * **Last Run:** `2026-05-29 21:11:56`
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
| <span style='color: green;'>16349238-d0d6-461d-ab13-cab729a921e2</span> | <span style='color: green;'>1</span> | <span style='color: green;'>(first_name: 'Alice', last_name: 'Meyer', city: 'Zurich', email: 'alice.meyer@example.com')</span>  | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>86705c3b-8883-4a50-808f-b4c92ba27ac7</span> | <span style='color: green;'>2</span> | <span style='color: green;'>(first_name: 'Bob', last_name: 'Keller', city: 'Bern', email: 'bob.keller@example.com')</span>      | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>7b223828-80f1-42ce-a175-a8be9a528635</span> | <span style='color: green;'>3</span> | <span style='color: green;'>(first_name: 'Clara', last_name: 'Schmid', city: 'Basel', email: 'clara.schmid@example.com')</span> | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |

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
| 16349238-d0d6-461d-ab13-cab729a921e2                                    | 1                                     | (first_name: 'Alice', last_name: 'Meyer', city: 'Zurich', email: 'alice.meyer@example.com')                                    | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                    | True                                    | True                                    | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                    |
| 86705c3b-8883-4a50-808f-b4c92ba27ac7                                    | 2                                     | (first_name: 'Bob', last_name: 'Keller', city: 'Bern', email: 'bob.keller@example.com')                                        | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                    | True                                    | True                                    | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                    |
| 7b223828-80f1-42ce-a175-a8be9a528635                                    | 3                                     | (first_name: 'Clara', last_name: 'Schmid', city: 'Basel', email: 'clara.schmid@example.com')                                   | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                    | True                                    | True                                    | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                    |
| <span style='color: green;'>e29fe033-e447-4158-bdcb-79d1c7b9eeea</span> | <span style='color: green;'>10</span> | <span style='color: green;'>(first_name: 'Kevin', last_name: 'Loosli', city: 'Bern', email: 'kevin.loosli@example.com')</span> | <span style='color: green;'>2026-01-05 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-06 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |

_the following columns where excluded from the result: `dp_record_hash`_

