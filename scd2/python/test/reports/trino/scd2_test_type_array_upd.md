# Testing Update Operation with a column of type ARRAY

This test validates an UPDATE operation of one entity (with a new version) on a set of existing entities.


 * **Strategy:** `trino`
 * **Last Run:** `2026-06-08 18:39:48`
## Test Step 1
At 2026-01-01 00:00:00, insert 3 entities into raw table and perform initial SCD2 merge.


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
| <span style='color: green;'>063b8208-7cad-45a2-a316-bf48723c27d3</span> | <span style='color: green;'>1</span> | <span style='color: green;'>['Alice', 'Meyer', 'Zurich', 'alice.meyer@example.com']</span>  | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>79da2aa3-ffff-4db6-ac73-859c472d4603</span> | <span style='color: green;'>2</span> | <span style='color: green;'>['Bob', 'Keller', 'Bern', 'bob.keller@example.com']</span>      | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>552e34d2-8afb-45e5-b40c-73e4eff6b078</span> | <span style='color: green;'>3</span> | <span style='color: green;'>['Clara', 'Schmid', 'Basel', 'clara.schmid@example.com']</span> | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |

_the following columns where excluded from the result: `dp_record_hash`_

## Test Step 2
At 2026-01-05 00:00:00, update `email` inside `user_info` of entity with `id=3` in raw table and perform SCD2 merge.


**Raw Table `raw_person`**


|   id | user_info                                                | status   | dp_ts_from          | dp_loaded_at        |
|------|----------------------------------------------------------|----------|---------------------|---------------------|
|    1 | ['Alice', 'Meyer', 'Zurich', 'alice.meyer@example.com']  | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    2 | ['Bob', 'Keller', 'Bern', 'bob.keller@example.com']      | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    3 | ['Clara', 'Schmid', 'Basel', 'clara.schmid@example.com'] | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    1 | ['Alice', 'Meyer', 'Zurich', 'alice.meyer@example.com']  | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    2 | ['Bob', 'Keller', 'Bern', 'bob.keller@example.com']      | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    3 | ['Clara', 'Schmid', 'Basel', 'clara.schmid@newmail.com'] | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |



**Input to Merge**


| merge_record_id                      | dp_record_id                         |   id | user_info                                                | dp_record_hash                                                   | dp_del_flag   | operation_type     | case_name   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   |
|--------------------------------------|--------------------------------------|------|----------------------------------------------------------|------------------------------------------------------------------|---------------|--------------------|-------------|---------------------|---------------------|----------------|----------------|
| 552e34d2-8afb-45e5-b40c-73e4eff6b078 | 552e34d2-8afb-45e5-b40c-73e4eff6b078 |    3 | ['Clara', 'Schmid', 'Basel', 'clara.schmid@newmail.com'] | 060C9A6BB75CBFD38D5DC29F1F56D8ADA388AF20E1321F1D9FC5B58072D5919B | ACTIVE        | UPDATE_VERSION     | CASE_11     | 2026-01-01 00:00:00 | 2026-01-04 23:59:59 | False          | False          |
| nan                                  | nan                                  |    3 | ['Clara', 'Schmid', 'Basel', 'clara.schmid@newmail.com'] | 060C9A6BB75CBFD38D5DC29F1F56D8ADA388AF20E1321F1D9FC5B58072D5919B | ACTIVE        | INSERT_NEW_VERSION | CASE_11     | 2026-01-05 00:00:00 | 9999-12-31 23:59:59 | True           | True           |



**Dimensional Table `dim_person`**


| dp_record_id                                                            | id                                   | user_info                                                                                   | dp_ts_from                                             | dp_ts_to                                                | dp_is_active                              | dp_is_latest                              | dp_load_ts                                             | dp_replace_ts                                           |
|-------------------------------------------------------------------------|--------------------------------------|---------------------------------------------------------------------------------------------|--------------------------------------------------------|---------------------------------------------------------|-------------------------------------------|-------------------------------------------|--------------------------------------------------------|---------------------------------------------------------|
| 063b8208-7cad-45a2-a316-bf48723c27d3                                    | 1                                    | ['Alice', 'Meyer', 'Zurich', 'alice.meyer@example.com']                                     | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                     | True                                      | True                                      | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                     |
| 79da2aa3-ffff-4db6-ac73-859c472d4603                                    | 2                                    | ['Bob', 'Keller', 'Bern', 'bob.keller@example.com']                                         | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                     | True                                      | True                                      | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                     |
| 552e34d2-8afb-45e5-b40c-73e4eff6b078                                    | 3                                    | ['Clara', 'Schmid', 'Basel', 'clara.schmid@example.com']                                    | 2026-01-01 00:00:00                                    | <span style='color: orange;'>2026-01-04 23:59:59</span> | <span style='color: orange;'>False</span> | <span style='color: orange;'>False</span> | 2026-01-02 00:00:00                                    | <span style='color: orange;'>2026-01-06 00:00:00</span> |
| <span style='color: green;'>49a814c3-3cbf-451d-998c-5ae839f32e17</span> | <span style='color: green;'>3</span> | <span style='color: green;'>['Clara', 'Schmid', 'Basel', 'clara.schmid@newmail.com']</span> | <span style='color: green;'>2026-01-05 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  | <span style='color: green;'>True</span>   | <span style='color: green;'>True</span>   | <span style='color: green;'>2026-01-06 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  |

_the following columns where excluded from the result: `dp_record_hash`_

