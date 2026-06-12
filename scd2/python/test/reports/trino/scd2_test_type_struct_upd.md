# Testing Update Operation with a column of type STRUCT

This test validates an UPDATE operation of one entity (with a new version) on a set of existing entities.


 * **Strategy:** `trino`
 * **Last Run:** `2026-06-09 16:45:09`
## Test Step 1
At 2026-01-01 00:00:00, insert 3 entities into raw table and perform initial SCD2 merge.


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
| <span style='color: green;'>117dfdc0-69f2-4200-b6d4-6a0bfe8f3f5a</span> | <span style='color: green;'>1</span> | <span style='color: green;'>(first_name: 'Alice', last_name: 'Meyer', city: 'Zurich', email: 'alice.meyer@example.com')</span>  | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>8c8930cc-e135-435e-905b-d03282bc429c</span> | <span style='color: green;'>2</span> | <span style='color: green;'>(first_name: 'Bob', last_name: 'Keller', city: 'Bern', email: 'bob.keller@example.com')</span>      | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>36183de3-1b3e-49e6-ad7c-3a122e70cba5</span> | <span style='color: green;'>3</span> | <span style='color: green;'>(first_name: 'Clara', last_name: 'Schmid', city: 'Basel', email: 'clara.schmid@example.com')</span> | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |

_the following columns where excluded from the result: `dp_record_hash`_

## Test Step 2
At 2026-01-05 00:00:00, update `email` inside `user_info` of entity with `id=3` in raw table and perform SCD2 merge.


**Raw Table `raw_person`**


|   id | user_info                                                                                    | status   | dp_ts_from          | dp_loaded_at        |
|------|----------------------------------------------------------------------------------------------|----------|---------------------|---------------------|
|    1 | (first_name: 'Alice', last_name: 'Meyer', city: 'Zurich', email: 'alice.meyer@example.com')  | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    2 | (first_name: 'Bob', last_name: 'Keller', city: 'Bern', email: 'bob.keller@example.com')      | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    3 | (first_name: 'Clara', last_name: 'Schmid', city: 'Basel', email: 'clara.schmid@example.com') | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    1 | (first_name: 'Alice', last_name: 'Meyer', city: 'Zurich', email: 'alice.meyer@example.com')  | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    2 | (first_name: 'Bob', last_name: 'Keller', city: 'Bern', email: 'bob.keller@example.com')      | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    3 | (first_name: 'Clara', last_name: 'Schmid', city: 'Basel', email: 'clara.schmid@newmail.com') | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |



**Input to Merge**


| merge_record_id                      | dp_record_id                         |   id | user_info                                                                                    | dp_record_hash                                                   | dp_del_flag   | operation_type     | case_name   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   |
|--------------------------------------|--------------------------------------|------|----------------------------------------------------------------------------------------------|------------------------------------------------------------------|---------------|--------------------|-------------|---------------------|---------------------|----------------|----------------|
| 36183de3-1b3e-49e6-ad7c-3a122e70cba5 | 36183de3-1b3e-49e6-ad7c-3a122e70cba5 |    3 | (first_name: 'Clara', last_name: 'Schmid', city: 'Basel', email: 'clara.schmid@newmail.com') | 9477D9000CEDC6AA3E01D45847CE658798640D2C2E3614371B6FA40923F369C6 | ACTIVE        | UPDATE_VERSION     | CASE_11     | 2026-01-01 00:00:00 | 2026-01-04 23:59:59 | False          | False          |
| nan                                  | nan                                  |    3 | (first_name: 'Clara', last_name: 'Schmid', city: 'Basel', email: 'clara.schmid@newmail.com') | 9477D9000CEDC6AA3E01D45847CE658798640D2C2E3614371B6FA40923F369C6 | ACTIVE        | INSERT_NEW_VERSION | CASE_11     | 2026-01-05 00:00:00 | 9999-12-31 23:59:59 | True           | True           |



**Dimensional Table `dim_person`**


| dp_record_id                                                            | id                                   | user_info                                                                                                                       | dp_ts_from                                             | dp_ts_to                                                | dp_is_active                              | dp_is_latest                              | dp_load_ts                                             | dp_replace_ts                                           |
|-------------------------------------------------------------------------|--------------------------------------|---------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------|---------------------------------------------------------|-------------------------------------------|-------------------------------------------|--------------------------------------------------------|---------------------------------------------------------|
| 117dfdc0-69f2-4200-b6d4-6a0bfe8f3f5a                                    | 1                                    | (first_name: 'Alice', last_name: 'Meyer', city: 'Zurich', email: 'alice.meyer@example.com')                                     | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                     | True                                      | True                                      | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                     |
| 8c8930cc-e135-435e-905b-d03282bc429c                                    | 2                                    | (first_name: 'Bob', last_name: 'Keller', city: 'Bern', email: 'bob.keller@example.com')                                         | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                     | True                                      | True                                      | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                     |
| 36183de3-1b3e-49e6-ad7c-3a122e70cba5                                    | 3                                    | (first_name: 'Clara', last_name: 'Schmid', city: 'Basel', email: 'clara.schmid@example.com')                                    | 2026-01-01 00:00:00                                    | <span style='color: orange;'>2026-01-04 23:59:59</span> | <span style='color: orange;'>False</span> | <span style='color: orange;'>False</span> | 2026-01-02 00:00:00                                    | <span style='color: orange;'>2026-01-06 00:00:00</span> |
| <span style='color: green;'>6f0c0c96-8be3-4c2d-8bb9-0a22dbdfc31c</span> | <span style='color: green;'>3</span> | <span style='color: green;'>(first_name: 'Clara', last_name: 'Schmid', city: 'Basel', email: 'clara.schmid@newmail.com')</span> | <span style='color: green;'>2026-01-05 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  | <span style='color: green;'>True</span>   | <span style='color: green;'>True</span>   | <span style='color: green;'>2026-01-06 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  |

_the following columns where excluded from the result: `dp_record_hash`_

