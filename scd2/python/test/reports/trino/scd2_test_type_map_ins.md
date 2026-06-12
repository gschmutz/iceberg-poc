# Testing Insert Operation with a column of type MAP

This test validates an INSERT operation of one new entity (with a 1st version) into a set of existing entities.


 * **Strategy:** `trino`
 * **Last Run:** `2026-06-09 16:44:34`
## Test Step 1
Insert 3 entities into raw table and perform initial SCD2 merge.


**Raw Table `raw_person`**


|   id | user_info                                                                                            | status   | dp_ts_from          | dp_loaded_at        |
|------|------------------------------------------------------------------------------------------------------|----------|---------------------|---------------------|
|    1 | {'first_name': 'Alice', 'last_name': 'Meyer', 'city': 'Zurich', 'email': 'alice.meyer@example.com'}  | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    2 | {'first_name': 'Bob', 'last_name': 'Keller', 'city': 'Bern', 'email': 'bob.keller@example.com'}      | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    3 | {'first_name': 'Clara', 'last_name': 'Schmid', 'city': 'Basel', 'email': 'clara.schmid@example.com'} | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |



**Input to Merge**


| merge_record_id   | dp_record_id   |   id | user_info                                                                                            | dp_record_hash                                                   | dp_del_flag   | operation_type     | case_name   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   |
|-------------------|----------------|------|------------------------------------------------------------------------------------------------------|------------------------------------------------------------------|---------------|--------------------|-------------|---------------------|---------------------|----------------|----------------|
|                   |                |    1 | {'first_name': 'Alice', 'last_name': 'Meyer', 'city': 'Zurich', 'email': 'alice.meyer@example.com'}  | F3EB9083D97A374919FF0C4FC913D263F9A437B1398888EA2D902A35D71DFF18 | ACTIVE        | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           |
|                   |                |    2 | {'first_name': 'Bob', 'last_name': 'Keller', 'city': 'Bern', 'email': 'bob.keller@example.com'}      | 8E22796C7A86F5A51D957BFC1CB1415692F924C4B9FE7497039C06F5BA26EB63 | ACTIVE        | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           |
|                   |                |    3 | {'first_name': 'Clara', 'last_name': 'Schmid', 'city': 'Basel', 'email': 'clara.schmid@example.com'} | D52C3A1221C144C1E50AECD6A257AA02EA146B298C8A4F1C771FCBE552E504E8 | ACTIVE        | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           |



**Dimensional Table `dim_person`**


| dp_record_id                                                            | id                                   | user_info                                                                                                                               | dp_ts_from                                             | dp_ts_to                                               | dp_is_active                            | dp_is_latest                            | dp_load_ts                                             | dp_replace_ts                                          |
|-------------------------------------------------------------------------|--------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------|--------------------------------------------------------|-----------------------------------------|-----------------------------------------|--------------------------------------------------------|--------------------------------------------------------|
| <span style='color: green;'>add908d2-fef3-4348-a1c3-893b5e3f0909</span> | <span style='color: green;'>1</span> | <span style='color: green;'>{'first_name': 'Alice', 'last_name': 'Meyer', 'city': 'Zurich', 'email': 'alice.meyer@example.com'}</span>  | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>6bd946e8-b50a-4283-858e-f81dd8f72df6</span> | <span style='color: green;'>2</span> | <span style='color: green;'>{'first_name': 'Bob', 'last_name': 'Keller', 'city': 'Bern', 'email': 'bob.keller@example.com'}</span>      | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>1988d0eb-f437-4e93-b072-920ae2bb94c3</span> | <span style='color: green;'>3</span> | <span style='color: green;'>{'first_name': 'Clara', 'last_name': 'Schmid', 'city': 'Basel', 'email': 'clara.schmid@example.com'}</span> | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |

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


| merge_record_id   | dp_record_id   |   id | user_info                                                                                           | dp_record_hash                                                   | dp_del_flag   | operation_type     | case_name   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   |
|-------------------|----------------|------|-----------------------------------------------------------------------------------------------------|------------------------------------------------------------------|---------------|--------------------|-------------|---------------------|---------------------|----------------|----------------|
|                   |                |   10 | {'first_name': 'Kevin', 'last_name': 'Loosli', 'city': 'Bern', 'email': 'kevin.loosli@example.com'} | D7A624F643B937EE218EB4F1ECA9AB207C61063D4AF96FBB22FFADB54292834D | ACTIVE        | INSERT_NEW_VERSION | CASE_1      | 2026-01-05 00:00:00 | 9999-12-31 23:59:59 | True           | True           |



**Dimensional Table `dim_person`**


| dp_record_id                                                            | id                                    | user_info                                                                                                                              | dp_ts_from                                             | dp_ts_to                                               | dp_is_active                            | dp_is_latest                            | dp_load_ts                                             | dp_replace_ts                                          |
|-------------------------------------------------------------------------|---------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------|--------------------------------------------------------|-----------------------------------------|-----------------------------------------|--------------------------------------------------------|--------------------------------------------------------|
| add908d2-fef3-4348-a1c3-893b5e3f0909                                    | 1                                     | {'first_name': 'Alice', 'last_name': 'Meyer', 'city': 'Zurich', 'email': 'alice.meyer@example.com'}                                    | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                    | True                                    | True                                    | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                    |
| 6bd946e8-b50a-4283-858e-f81dd8f72df6                                    | 2                                     | {'first_name': 'Bob', 'last_name': 'Keller', 'city': 'Bern', 'email': 'bob.keller@example.com'}                                        | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                    | True                                    | True                                    | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                    |
| 1988d0eb-f437-4e93-b072-920ae2bb94c3                                    | 3                                     | {'first_name': 'Clara', 'last_name': 'Schmid', 'city': 'Basel', 'email': 'clara.schmid@example.com'}                                   | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                    | True                                    | True                                    | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                    |
| <span style='color: green;'>3b05f1ff-34c6-41eb-861a-4db23c7b3641</span> | <span style='color: green;'>10</span> | <span style='color: green;'>{'first_name': 'Kevin', 'last_name': 'Loosli', 'city': 'Bern', 'email': 'kevin.loosli@example.com'}</span> | <span style='color: green;'>2026-01-05 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-06 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |

_the following columns where excluded from the result: `dp_record_hash`_

