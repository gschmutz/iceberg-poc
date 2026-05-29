# Testing Logical Delete Operation with many versions

This test validates a DELETE operation of a single entity with many versions. The delete is created by a logical delete in the raw table, i.e., the entity's status is set to 'INACTIVE'.


 * **Strategy:** `pyspark`
 * **Last Run:** `2026-05-07 14:24:24`
## Test Step 1
At 2026-01-01 00:00:00, insert 3 entities into raw table and perform initial SCD2 merge. At 2026-01-05 00:00:00, update email address of entity with `id=3` and perform SCD2 merge.


**Raw Table `raw_person`**


|   id | first_name   | last_name   | city   | email                    | status   | dp_ts_from          | dp_loaded_at        |
|------|--------------|-------------|--------|--------------------------|----------|---------------------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@newmail.com | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |



**Input to Merge**


| merge_record_id                      | dp_record_id                         |   id | first_name   | last_name   | city   | email                    | dp_record_hash                                                   | dp_del_flag   | operation_type     | case_name   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   |
|--------------------------------------|--------------------------------------|------|--------------|-------------|--------|--------------------------|------------------------------------------------------------------|---------------|--------------------|-------------|---------------------|---------------------|----------------|----------------|
| 544671fd-d545-48ec-a209-0b7d11442fdb | 544671fd-d545-48ec-a209-0b7d11442fdb |    3 | Clara        | Schmid      | Basel  | clara.schmid@newmail.com | 38315ECF14E9962283673DAFA2DFF3A93149B4C245591F3C6758E356EDBDE696 | ACTIVE        | UPDATE_VERSION     | CASE_11     | 2026-01-01 00:00:00 | 2026-01-04 23:59:59 | False          | False          |
|                                      | 83612485-58c1-451c-b5c0-35a89a29ce94 |    3 | Clara        | Schmid      | Basel  | clara.schmid@newmail.com | 38315ECF14E9962283673DAFA2DFF3A93149B4C245591F3C6758E356EDBDE696 | ACTIVE        | INSERT_NEW_VERSION | CASE_11     | 2026-01-05 00:00:00 | 9999-12-31 23:59:59 | True           | True           |



**Dimensional Table `dim_person`**


| dp_record_id                                                            | id                                   | first_name                               | last_name                                 | city                                     | email                                                       | dp_ts_from                                             | dp_ts_to                                                | dp_is_active                              | dp_is_latest                              | dp_load_ts                                             | dp_replace_ts                                           |
|-------------------------------------------------------------------------|--------------------------------------|------------------------------------------|-------------------------------------------|------------------------------------------|-------------------------------------------------------------|--------------------------------------------------------|---------------------------------------------------------|-------------------------------------------|-------------------------------------------|--------------------------------------------------------|---------------------------------------------------------|
| 25bd2769-4051-48da-8188-c3a5a99a6998                                    | 1                                    | Alice                                    | Meyer                                     | Zurich                                   | alice.meyer@example.com                                     | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                     | True                                      | True                                      | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                     |
| 85207b35-7085-4f69-acaf-1305898c0863                                    | 2                                    | Bob                                      | Keller                                    | Bern                                     | bob.keller@example.com                                      | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                     | True                                      | True                                      | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                     |
| 544671fd-d545-48ec-a209-0b7d11442fdb                                    | 3                                    | Clara                                    | Schmid                                    | Basel                                    | clara.schmid@example.com                                    | 2026-01-01 00:00:00                                    | <span style='color: orange;'>2026-01-04 23:59:59</span> | <span style='color: orange;'>False</span> | <span style='color: orange;'>False</span> | 2026-01-02 00:00:00                                    | <span style='color: orange;'>2026-01-06 00:00:00</span> |
| <span style='color: green;'>83612485-58c1-451c-b5c0-35a89a29ce94</span> | <span style='color: green;'>3</span> | <span style='color: green;'>Clara</span> | <span style='color: green;'>Schmid</span> | <span style='color: green;'>Basel</span> | <span style='color: green;'>clara.schmid@newmail.com</span> | <span style='color: green;'>2026-01-05 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  | <span style='color: green;'>True</span>   | <span style='color: green;'>True</span>   | <span style='color: green;'>2026-01-06 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  |

_the following columns where excluded from the result: `dp_record_hash`_

## Test Step 2
At 2026-01-10 00:00:00, delete entity with `id=3` from raw table (logical delete) and perform SCD2 merge. The active version of the entity with `id=3` should be marked as DELETED with `dp_ts_to` = 2026-01-10 00:00:00 - 1 second and `dp_is_active` = False.


**Raw Table `raw_person`**


|   id | first_name   | last_name   | city   | email                    | status   | dp_ts_from          | dp_loaded_at        |
|------|--------------|-------------|--------|--------------------------|----------|---------------------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@newmail.com | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-10 00:00:00 | 2026-01-10 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-10 00:00:00 | 2026-01-10 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@newmail.com | INACTIVE | 2026-01-10 00:00:00 | 2026-01-10 00:00:00 |



**Input to Merge**


| merge_record_id                      | dp_record_id                         |   id | first_name   | last_name   | city   | email                    | dp_record_hash                                                   | dp_del_flag   | operation_type   | case_name   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   |
|--------------------------------------|--------------------------------------|------|--------------|-------------|--------|--------------------------|------------------------------------------------------------------|---------------|------------------|-------------|---------------------|---------------------|----------------|----------------|
| 83612485-58c1-451c-b5c0-35a89a29ce94 | 83612485-58c1-451c-b5c0-35a89a29ce94 |    3 | Clara        | Schmid      | Basel  | clara.schmid@newmail.com | 38315ECF14E9962283673DAFA2DFF3A93149B4C245591F3C6758E356EDBDE696 | INACTIVE      | UPDATE_VERSION   | CASE_30     | 2026-01-05 00:00:00 | 2026-01-09 23:59:59 | False          | True           |



**Dimensional Table `dim_person`**


| dp_record_id                         |   id | first_name   | last_name   | city   | email                    | dp_ts_from          | dp_ts_to                                                | dp_is_active                              | dp_is_latest   | dp_load_ts          | dp_replace_ts                                           |
|--------------------------------------|------|--------------|-------------|--------|--------------------------|---------------------|---------------------------------------------------------|-------------------------------------------|----------------|---------------------|---------------------------------------------------------|
| 25bd2769-4051-48da-8188-c3a5a99a6998 |    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | 2026-01-01 00:00:00 | 9999-12-31 23:59:59                                     | True                                      | True           | 2026-01-02 00:00:00 | 9999-12-31 23:59:59                                     |
| 85207b35-7085-4f69-acaf-1305898c0863 |    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | 2026-01-01 00:00:00 | 9999-12-31 23:59:59                                     | True                                      | True           | 2026-01-02 00:00:00 | 9999-12-31 23:59:59                                     |
| 544671fd-d545-48ec-a209-0b7d11442fdb |    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | 2026-01-01 00:00:00 | 2026-01-04 23:59:59                                     | False                                     | False          | 2026-01-02 00:00:00 | 2026-01-06 00:00:00                                     |
| 83612485-58c1-451c-b5c0-35a89a29ce94 |    3 | Clara        | Schmid      | Basel  | clara.schmid@newmail.com | 2026-01-05 00:00:00 | <span style='color: orange;'>2026-01-09 23:59:59</span> | <span style='color: orange;'>False</span> | True           | 2026-01-06 00:00:00 | <span style='color: orange;'>2026-01-11 00:00:00</span> |

_the following columns where excluded from the result: `dp_record_hash`_

