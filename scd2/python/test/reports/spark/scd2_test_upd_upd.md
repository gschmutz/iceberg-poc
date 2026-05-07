# Testing Multiple Update Operations on same entity but different fields

This test validates multiple UPDATE operations on one entity over time producing many versions.


 * **Strategy:** `spark`
 * **Last Run:** `2026-05-07 13:58:23`
## Test Step 1
Insert 2 entities into raw table and perform initial SCD2 merge.


**Raw Table `raw_person`**


|   id | first_name   | last_name   | city   | email                   | status   | dp_ts_from          | dp_loaded_at        |
|------|--------------|-------------|--------|-------------------------|----------|---------------------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com  | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |



**Input to Merge**


| merge_record_id   | dp_record_id                         |   id | first_name   | last_name   | city   | email                   | dp_record_hash                                                   | dp_del_flag   | operation_type     | case_name   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   |
|-------------------|--------------------------------------|------|--------------|-------------|--------|-------------------------|------------------------------------------------------------------|---------------|--------------------|-------------|---------------------|---------------------|----------------|----------------|
|                   | 54e74755-9746-4049-b40a-dd984de7628c |    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com | 00B9A7122065F01BE7FD23C6FB962AEE6DE3B84D0BA50409DC26FC5A150FBDC8 | ACTIVE        | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           |
|                   | 1ba2dc9e-775b-4387-ae93-a580753fd7bd |    2 | Bob          | Keller      | Bern   | bob.keller@example.com  | D28A23C8422275E006FCF3D86AA51CF4E058FB495B8E48560FC9BF7BCC019B40 | ACTIVE        | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           |



**Dimensional Table `dim_person`**


| dp_record_id                                                            | id                                   | first_name                               | last_name                                 | city                                      | email                                                      | dp_ts_from                                             | dp_ts_to                                               | dp_is_active                            | dp_is_latest                            | dp_load_ts                                             | dp_replace_ts                                          |
|-------------------------------------------------------------------------|--------------------------------------|------------------------------------------|-------------------------------------------|-------------------------------------------|------------------------------------------------------------|--------------------------------------------------------|--------------------------------------------------------|-----------------------------------------|-----------------------------------------|--------------------------------------------------------|--------------------------------------------------------|
| <span style='color: green;'>92f37520-fdb7-49de-bcdc-1086ca8bde83</span> | <span style='color: green;'>1</span> | <span style='color: green;'>Alice</span> | <span style='color: green;'>Meyer</span>  | <span style='color: green;'>Zurich</span> | <span style='color: green;'>alice.meyer@example.com</span> | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>93fdd584-85ed-4150-95eb-964c6b62da69</span> | <span style='color: green;'>2</span> | <span style='color: green;'>Bob</span>   | <span style='color: green;'>Keller</span> | <span style='color: green;'>Bern</span>   | <span style='color: green;'>bob.keller@example.com</span>  | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |

_the following columns where excluded from the result: `dp_record_hash`_

## Test Step 2
At 2026-01-05 00:00:00, update `city` of entity with `id=1` and perform SCD2 merge.


**Raw Table `raw_person`**


|   id | first_name   | last_name   | city   | email                   | status   | dp_ts_from          | dp_loaded_at        |
|------|--------------|-------------|--------|-------------------------|----------|---------------------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com  | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    1 | Alice        | Meyer       | Bern   | alice.meyer@example.com | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com  | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |



**Input to Merge**


| merge_record_id                      | dp_record_id                         |   id | first_name   | last_name   | city   | email                   | dp_record_hash                                                   | dp_del_flag   | operation_type     | case_name   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   |
|--------------------------------------|--------------------------------------|------|--------------|-------------|--------|-------------------------|------------------------------------------------------------------|---------------|--------------------|-------------|---------------------|---------------------|----------------|----------------|
| 92f37520-fdb7-49de-bcdc-1086ca8bde83 | 92f37520-fdb7-49de-bcdc-1086ca8bde83 |    1 | Alice        | Meyer       | Bern   | alice.meyer@example.com | 6449C8A21EC1B7B2BD4891618CF5853B27A97968D41570EE3CD34617BDBBD7BD | ACTIVE        | UPDATE_VERSION     | CASE_11     | 2026-01-01 00:00:00 | 2026-01-04 23:59:59 | False          | False          |
|                                      | d661639b-372a-410f-baeb-87b9e83697fc |    1 | Alice        | Meyer       | Bern   | alice.meyer@example.com | 6449C8A21EC1B7B2BD4891618CF5853B27A97968D41570EE3CD34617BDBBD7BD | ACTIVE        | INSERT_NEW_VERSION | CASE_11     | 2026-01-05 00:00:00 | 9999-12-31 23:59:59 | True           | True           |



**Dimensional Table `dim_person`**


| dp_record_id                                                            | id                                   | first_name                               | last_name                                | city                                    | email                                                      | dp_ts_from                                             | dp_ts_to                                                | dp_is_active                              | dp_is_latest                              | dp_load_ts                                             | dp_replace_ts                                           |
|-------------------------------------------------------------------------|--------------------------------------|------------------------------------------|------------------------------------------|-----------------------------------------|------------------------------------------------------------|--------------------------------------------------------|---------------------------------------------------------|-------------------------------------------|-------------------------------------------|--------------------------------------------------------|---------------------------------------------------------|
| 92f37520-fdb7-49de-bcdc-1086ca8bde83                                    | 1                                    | Alice                                    | Meyer                                    | Zurich                                  | alice.meyer@example.com                                    | 2026-01-01 00:00:00                                    | <span style='color: orange;'>2026-01-04 23:59:59</span> | <span style='color: orange;'>False</span> | <span style='color: orange;'>False</span> | 2026-01-02 00:00:00                                    | <span style='color: orange;'>2026-01-06 00:00:00</span> |
| <span style='color: green;'>ab5edd47-dfa4-4183-baeb-ff41ad199ad4</span> | <span style='color: green;'>1</span> | <span style='color: green;'>Alice</span> | <span style='color: green;'>Meyer</span> | <span style='color: green;'>Bern</span> | <span style='color: green;'>alice.meyer@example.com</span> | <span style='color: green;'>2026-01-05 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  | <span style='color: green;'>True</span>   | <span style='color: green;'>True</span>   | <span style='color: green;'>2026-01-06 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  |
| 93fdd584-85ed-4150-95eb-964c6b62da69                                    | 2                                    | Bob                                      | Keller                                   | Bern                                    | bob.keller@example.com                                     | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                     | True                                      | True                                      | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                     |

_the following columns where excluded from the result: `dp_record_hash`_

## Test Step 3
At 2026-01-10 00:00:00, update `email` of entity with `id=1` and perform SCD2 merge.


**Raw Table `raw_person`**


|   id | first_name   | last_name   | city   | email                   | status   | dp_ts_from          | dp_loaded_at        |
|------|--------------|-------------|--------|-------------------------|----------|---------------------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com  | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    1 | Alice        | Meyer       | Bern   | alice.meyer@example.com | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com  | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    1 | Alice        | Meyer       | Bern   | alice.meyer@newmail.com | ACTIVE   | 2026-01-10 00:00:00 | 2026-01-10 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com  | ACTIVE   | 2026-01-10 00:00:00 | 2026-01-10 00:00:00 |



**Input to Merge**


| merge_record_id                      | dp_record_id                         |   id | first_name   | last_name   | city   | email                   | dp_record_hash                                                   | dp_del_flag   | operation_type     | case_name   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   |
|--------------------------------------|--------------------------------------|------|--------------|-------------|--------|-------------------------|------------------------------------------------------------------|---------------|--------------------|-------------|---------------------|---------------------|----------------|----------------|
| ab5edd47-dfa4-4183-baeb-ff41ad199ad4 | ab5edd47-dfa4-4183-baeb-ff41ad199ad4 |    1 | Alice        | Meyer       | Bern   | alice.meyer@newmail.com | 658A2A3D277000AE5C47A2B7CD098D22A6CC6CB17582E31856ABC52816413408 | ACTIVE        | UPDATE_VERSION     | CASE_11     | 2026-01-05 00:00:00 | 2026-01-09 23:59:59 | False          | False          |
|                                      | 3c7e0e27-6152-4372-8025-cdeee0ce7795 |    1 | Alice        | Meyer       | Bern   | alice.meyer@newmail.com | 658A2A3D277000AE5C47A2B7CD098D22A6CC6CB17582E31856ABC52816413408 | ACTIVE        | INSERT_NEW_VERSION | CASE_11     | 2026-01-10 00:00:00 | 9999-12-31 23:59:59 | True           | True           |



**Dimensional Table `dim_person`**


| dp_record_id                                                            | id                                   | first_name                               | last_name                                | city                                    | email                                                      | dp_ts_from                                             | dp_ts_to                                                | dp_is_active                              | dp_is_latest                              | dp_load_ts                                             | dp_replace_ts                                           |
|-------------------------------------------------------------------------|--------------------------------------|------------------------------------------|------------------------------------------|-----------------------------------------|------------------------------------------------------------|--------------------------------------------------------|---------------------------------------------------------|-------------------------------------------|-------------------------------------------|--------------------------------------------------------|---------------------------------------------------------|
| 92f37520-fdb7-49de-bcdc-1086ca8bde83                                    | 1                                    | Alice                                    | Meyer                                    | Zurich                                  | alice.meyer@example.com                                    | 2026-01-01 00:00:00                                    | 2026-01-04 23:59:59                                     | False                                     | False                                     | 2026-01-02 00:00:00                                    | 2026-01-06 00:00:00                                     |
| ab5edd47-dfa4-4183-baeb-ff41ad199ad4                                    | 1                                    | Alice                                    | Meyer                                    | Bern                                    | alice.meyer@example.com                                    | 2026-01-05 00:00:00                                    | <span style='color: orange;'>2026-01-09 23:59:59</span> | <span style='color: orange;'>False</span> | <span style='color: orange;'>False</span> | 2026-01-06 00:00:00                                    | <span style='color: orange;'>2026-01-11 00:00:00</span> |
| <span style='color: green;'>24127706-c6af-4273-a457-5fbf4acb8183</span> | <span style='color: green;'>1</span> | <span style='color: green;'>Alice</span> | <span style='color: green;'>Meyer</span> | <span style='color: green;'>Bern</span> | <span style='color: green;'>alice.meyer@newmail.com</span> | <span style='color: green;'>2026-01-10 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  | <span style='color: green;'>True</span>   | <span style='color: green;'>True</span>   | <span style='color: green;'>2026-01-11 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  |
| 93fdd584-85ed-4150-95eb-964c6b62da69                                    | 2                                    | Bob                                      | Keller                                   | Bern                                    | bob.keller@example.com                                     | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                     | True                                      | True                                      | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                     |

_the following columns where excluded from the result: `dp_record_hash`_

## Test Step 4
At 2026-01-20 00:00:00, update `last_name` of entity with `id=1` and perform SCD2 merge.


**Raw Table `raw_person`**


|   id | first_name   | last_name    | city   | email                   | status   | dp_ts_from          | dp_loaded_at        |
|------|--------------|--------------|--------|-------------------------|----------|---------------------|---------------------|
|    1 | Alice        | Meyer        | Zurich | alice.meyer@example.com | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    2 | Bob          | Keller       | Bern   | bob.keller@example.com  | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    1 | Alice        | Meyer        | Bern   | alice.meyer@example.com | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    2 | Bob          | Keller       | Bern   | bob.keller@example.com  | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    1 | Alice        | Meyer        | Bern   | alice.meyer@newmail.com | ACTIVE   | 2026-01-10 00:00:00 | 2026-01-10 00:00:00 |
|    2 | Bob          | Keller       | Bern   | bob.keller@example.com  | ACTIVE   | 2026-01-10 00:00:00 | 2026-01-10 00:00:00 |
|    1 | Alice        | Müller-Meyer | Bern   | alice.meyer@newmail.com | ACTIVE   | 2026-01-20 00:00:00 | 2026-01-20 00:00:00 |
|    2 | Bob          | Keller       | Bern   | bob.keller@example.com  | ACTIVE   | 2026-01-20 00:00:00 | 2026-01-20 00:00:00 |



**Input to Merge**


| merge_record_id                      | dp_record_id                         |   id | first_name   | last_name    | city   | email                   | dp_record_hash                                                   | dp_del_flag   | operation_type     | case_name   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   |
|--------------------------------------|--------------------------------------|------|--------------|--------------|--------|-------------------------|------------------------------------------------------------------|---------------|--------------------|-------------|---------------------|---------------------|----------------|----------------|
| 24127706-c6af-4273-a457-5fbf4acb8183 | 24127706-c6af-4273-a457-5fbf4acb8183 |    1 | Alice        | Müller-Meyer | Bern   | alice.meyer@newmail.com | A69C37D41D4DF8E73BBE9CDEFBDA2EFFA89FB91D271E656B3020F83F0F13E7B8 | ACTIVE        | UPDATE_VERSION     | CASE_11     | 2026-01-10 00:00:00 | 2026-01-19 23:59:59 | False          | False          |
|                                      | 567a8e99-7499-497e-b4ca-09c2ac0b96ff |    1 | Alice        | Müller-Meyer | Bern   | alice.meyer@newmail.com | A69C37D41D4DF8E73BBE9CDEFBDA2EFFA89FB91D271E656B3020F83F0F13E7B8 | ACTIVE        | INSERT_NEW_VERSION | CASE_11     | 2026-01-20 00:00:00 | 9999-12-31 23:59:59 | True           | True           |



**Dimensional Table `dim_person`**


| dp_record_id                                                            | id                                   | first_name                               | last_name                                       | city                                    | email                                                      | dp_ts_from                                             | dp_ts_to                                                | dp_is_active                              | dp_is_latest                              | dp_load_ts                                             | dp_replace_ts                                           |
|-------------------------------------------------------------------------|--------------------------------------|------------------------------------------|-------------------------------------------------|-----------------------------------------|------------------------------------------------------------|--------------------------------------------------------|---------------------------------------------------------|-------------------------------------------|-------------------------------------------|--------------------------------------------------------|---------------------------------------------------------|
| 92f37520-fdb7-49de-bcdc-1086ca8bde83                                    | 1                                    | Alice                                    | Meyer                                           | Zurich                                  | alice.meyer@example.com                                    | 2026-01-01 00:00:00                                    | 2026-01-04 23:59:59                                     | False                                     | False                                     | 2026-01-02 00:00:00                                    | 2026-01-06 00:00:00                                     |
| ab5edd47-dfa4-4183-baeb-ff41ad199ad4                                    | 1                                    | Alice                                    | Meyer                                           | Bern                                    | alice.meyer@example.com                                    | 2026-01-05 00:00:00                                    | 2026-01-09 23:59:59                                     | False                                     | False                                     | 2026-01-06 00:00:00                                    | 2026-01-11 00:00:00                                     |
| 24127706-c6af-4273-a457-5fbf4acb8183                                    | 1                                    | Alice                                    | Meyer                                           | Bern                                    | alice.meyer@newmail.com                                    | 2026-01-10 00:00:00                                    | <span style='color: orange;'>2026-01-19 23:59:59</span> | <span style='color: orange;'>False</span> | <span style='color: orange;'>False</span> | 2026-01-11 00:00:00                                    | <span style='color: orange;'>2026-01-21 00:00:00</span> |
| <span style='color: green;'>5597dcfe-2aac-4ad1-a3f1-1d1d8eae423a</span> | <span style='color: green;'>1</span> | <span style='color: green;'>Alice</span> | <span style='color: green;'>Müller-Meyer</span> | <span style='color: green;'>Bern</span> | <span style='color: green;'>alice.meyer@newmail.com</span> | <span style='color: green;'>2026-01-20 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  | <span style='color: green;'>True</span>   | <span style='color: green;'>True</span>   | <span style='color: green;'>2026-01-21 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  |
| 93fdd584-85ed-4150-95eb-964c6b62da69                                    | 2                                    | Bob                                      | Keller                                          | Bern                                    | bob.keller@example.com                                     | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                     | True                                      | True                                      | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                     |

_the following columns where excluded from the result: `dp_record_hash`_

