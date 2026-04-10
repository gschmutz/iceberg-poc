# Testing Multiple Update Operations on same entity but different fields

This test validates multiple UPDATE operations on one entity over time producing many versions.


 * **Strategy:** `pyspark`
 * **Last Run:** `2026-04-10 14:33:39`
## Test Step 1
Insert 2 entities into raw table and perform initial SCD2 merge.


**Raw Table `raw_person`**


|   id | first_name   | last_name   | city   | email                   | status   | dp_ts_from          | dp_loaded_at        |
|------|--------------|-------------|--------|-------------------------|----------|---------------------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com  | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |



**Input to Merge**


| merge_key   | dp_key                               |   id | first_name   | last_name   | city   | email                   | record_hash                                                      | load_ts             | status   | operation_type     | case_name   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   |
|-------------|--------------------------------------|------|--------------|-------------|--------|-------------------------|------------------------------------------------------------------|---------------------|----------|--------------------|-------------|---------------------|---------------------|----------------|----------------|
|             | 4e7aeb65-70c4-43a9-8e85-7d9f8af9f542 |    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com | 00B9A7122065F01BE7FD23C6FB962AEE6DE3B84D0BA50409DC26FC5A150FBDC8 | 2026-01-01 00:00:00 | ACTIVE   | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           |
|             | 8766f706-c9d5-4b39-9a1b-deeae1e99cf7 |    2 | Bob          | Keller      | Bern   | bob.keller@example.com  | D28A23C8422275E006FCF3D86AA51CF4E058FB495B8E48560FC9BF7BCC019B40 | 2026-01-01 00:00:00 | ACTIVE   | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           |



**Dimensional Table `dim_person`**


| dp_key                                                                  | id                                   | first_name                               | last_name                                 | city                                      | email                                                      | dp_ts_from                                             | dp_ts_to                                               | dp_is_active                            | dp_is_latest                            | dp_created_at                                          | dp_replaced_at                                         |
|-------------------------------------------------------------------------|--------------------------------------|------------------------------------------|-------------------------------------------|-------------------------------------------|------------------------------------------------------------|--------------------------------------------------------|--------------------------------------------------------|-----------------------------------------|-----------------------------------------|--------------------------------------------------------|--------------------------------------------------------|
| <span style='color: green;'>4e7aeb65-70c4-43a9-8e85-7d9f8af9f542</span> | <span style='color: green;'>1</span> | <span style='color: green;'>Alice</span> | <span style='color: green;'>Meyer</span>  | <span style='color: green;'>Zurich</span> | <span style='color: green;'>alice.meyer@example.com</span> | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>8766f706-c9d5-4b39-9a1b-deeae1e99cf7</span> | <span style='color: green;'>2</span> | <span style='color: green;'>Bob</span>   | <span style='color: green;'>Keller</span> | <span style='color: green;'>Bern</span>   | <span style='color: green;'>bob.keller@example.com</span>  | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |

_the following columns where excluded from the result: `record_hash, dp_load_timestamp, change_type`_

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


| merge_key                            | dp_key                               |   id | first_name   | last_name   | city   | email                   | record_hash                                                      | load_ts             | status   | operation_type     | case_name   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   |
|--------------------------------------|--------------------------------------|------|--------------|-------------|--------|-------------------------|------------------------------------------------------------------|---------------------|----------|--------------------|-------------|---------------------|---------------------|----------------|----------------|
| 4e7aeb65-70c4-43a9-8e85-7d9f8af9f542 | 4e7aeb65-70c4-43a9-8e85-7d9f8af9f542 |    1 | Alice        | Meyer       | Bern   | alice.meyer@example.com | 6449C8A21EC1B7B2BD4891618CF5853B27A97968D41570EE3CD34617BDBBD7BD | 2026-01-05 00:00:00 | ACTIVE   | UPDATE_VERSION     | CASE_11     | 2026-01-01 00:00:00 | 2026-01-04 23:59:59 | False          | False          |
|                                      | d3ec9197-1256-4101-a9d9-040019ad8c66 |    1 | Alice        | Meyer       | Bern   | alice.meyer@example.com | 6449C8A21EC1B7B2BD4891618CF5853B27A97968D41570EE3CD34617BDBBD7BD | 2026-01-05 00:00:00 | ACTIVE   | INSERT_NEW_VERSION | CASE_11     | 2026-01-05 00:00:00 | 9999-12-31 23:59:59 | True           | True           |



**Dimensional Table `dim_person`**


| dp_key                                                                  | id                                   | first_name                               | last_name                                | city                                    | email                                                      | dp_ts_from                                             | dp_ts_to                                                | dp_is_active                              | dp_is_latest                              | dp_created_at                                          | dp_replaced_at                                          |
|-------------------------------------------------------------------------|--------------------------------------|------------------------------------------|------------------------------------------|-----------------------------------------|------------------------------------------------------------|--------------------------------------------------------|---------------------------------------------------------|-------------------------------------------|-------------------------------------------|--------------------------------------------------------|---------------------------------------------------------|
| 4e7aeb65-70c4-43a9-8e85-7d9f8af9f542                                    | 1                                    | Alice                                    | Meyer                                    | Zurich                                  | alice.meyer@example.com                                    | 2026-01-01 00:00:00                                    | <span style='color: orange;'>2026-01-04 23:59:59</span> | <span style='color: orange;'>False</span> | <span style='color: orange;'>False</span> | 2026-01-02 00:00:00                                    | <span style='color: orange;'>2026-01-06 00:00:00</span> |
| <span style='color: green;'>d3ec9197-1256-4101-a9d9-040019ad8c66</span> | <span style='color: green;'>1</span> | <span style='color: green;'>Alice</span> | <span style='color: green;'>Meyer</span> | <span style='color: green;'>Bern</span> | <span style='color: green;'>alice.meyer@example.com</span> | <span style='color: green;'>2026-01-05 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  | <span style='color: green;'>True</span>   | <span style='color: green;'>True</span>   | <span style='color: green;'>2026-01-06 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  |
| 8766f706-c9d5-4b39-9a1b-deeae1e99cf7                                    | 2                                    | Bob                                      | Keller                                   | Bern                                    | bob.keller@example.com                                     | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                     | True                                      | True                                      | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                     |

_the following columns where excluded from the result: `record_hash, dp_load_timestamp, change_type`_

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


| merge_key                            | dp_key                               |   id | first_name   | last_name   | city   | email                   | record_hash                                                      | load_ts             | status   | operation_type     | case_name   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   |
|--------------------------------------|--------------------------------------|------|--------------|-------------|--------|-------------------------|------------------------------------------------------------------|---------------------|----------|--------------------|-------------|---------------------|---------------------|----------------|----------------|
| d3ec9197-1256-4101-a9d9-040019ad8c66 | d3ec9197-1256-4101-a9d9-040019ad8c66 |    1 | Alice        | Meyer       | Bern   | alice.meyer@newmail.com | 658A2A3D277000AE5C47A2B7CD098D22A6CC6CB17582E31856ABC52816413408 | 2026-01-10 00:00:00 | ACTIVE   | UPDATE_VERSION     | CASE_11     | 2026-01-05 00:00:00 | 2026-01-09 23:59:59 | False          | False          |
|                                      | 72aad207-cab3-423a-a2da-63c87386e48c |    1 | Alice        | Meyer       | Bern   | alice.meyer@newmail.com | 658A2A3D277000AE5C47A2B7CD098D22A6CC6CB17582E31856ABC52816413408 | 2026-01-10 00:00:00 | ACTIVE   | INSERT_NEW_VERSION | CASE_11     | 2026-01-10 00:00:00 | 9999-12-31 23:59:59 | True           | True           |



**Dimensional Table `dim_person`**


| dp_key                                                                  | id                                   | first_name                               | last_name                                | city                                    | email                                                      | dp_ts_from                                             | dp_ts_to                                                | dp_is_active                              | dp_is_latest                              | dp_created_at                                          | dp_replaced_at                                          |
|-------------------------------------------------------------------------|--------------------------------------|------------------------------------------|------------------------------------------|-----------------------------------------|------------------------------------------------------------|--------------------------------------------------------|---------------------------------------------------------|-------------------------------------------|-------------------------------------------|--------------------------------------------------------|---------------------------------------------------------|
| 4e7aeb65-70c4-43a9-8e85-7d9f8af9f542                                    | 1                                    | Alice                                    | Meyer                                    | Zurich                                  | alice.meyer@example.com                                    | 2026-01-01 00:00:00                                    | 2026-01-04 23:59:59                                     | False                                     | False                                     | 2026-01-02 00:00:00                                    | 2026-01-06 00:00:00                                     |
| d3ec9197-1256-4101-a9d9-040019ad8c66                                    | 1                                    | Alice                                    | Meyer                                    | Bern                                    | alice.meyer@example.com                                    | 2026-01-05 00:00:00                                    | <span style='color: orange;'>2026-01-09 23:59:59</span> | <span style='color: orange;'>False</span> | <span style='color: orange;'>False</span> | 2026-01-06 00:00:00                                    | <span style='color: orange;'>2026-01-11 00:00:00</span> |
| <span style='color: green;'>72aad207-cab3-423a-a2da-63c87386e48c</span> | <span style='color: green;'>1</span> | <span style='color: green;'>Alice</span> | <span style='color: green;'>Meyer</span> | <span style='color: green;'>Bern</span> | <span style='color: green;'>alice.meyer@newmail.com</span> | <span style='color: green;'>2026-01-10 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  | <span style='color: green;'>True</span>   | <span style='color: green;'>True</span>   | <span style='color: green;'>2026-01-11 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  |
| 8766f706-c9d5-4b39-9a1b-deeae1e99cf7                                    | 2                                    | Bob                                      | Keller                                   | Bern                                    | bob.keller@example.com                                     | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                     | True                                      | True                                      | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                     |

_the following columns where excluded from the result: `record_hash, dp_load_timestamp, change_type`_

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


| merge_key                            | dp_key                               |   id | first_name   | last_name    | city   | email                   | record_hash                                                      | load_ts             | status   | operation_type     | case_name   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   |
|--------------------------------------|--------------------------------------|------|--------------|--------------|--------|-------------------------|------------------------------------------------------------------|---------------------|----------|--------------------|-------------|---------------------|---------------------|----------------|----------------|
| 72aad207-cab3-423a-a2da-63c87386e48c | 72aad207-cab3-423a-a2da-63c87386e48c |    1 | Alice        | Müller-Meyer | Bern   | alice.meyer@newmail.com | A69C37D41D4DF8E73BBE9CDEFBDA2EFFA89FB91D271E656B3020F83F0F13E7B8 | 2026-01-20 00:00:00 | ACTIVE   | UPDATE_VERSION     | CASE_11     | 2026-01-10 00:00:00 | 2026-01-19 23:59:59 | False          | False          |
|                                      | aa9f32c2-6c72-4dc6-a53b-70a1532ce7f5 |    1 | Alice        | Müller-Meyer | Bern   | alice.meyer@newmail.com | A69C37D41D4DF8E73BBE9CDEFBDA2EFFA89FB91D271E656B3020F83F0F13E7B8 | 2026-01-20 00:00:00 | ACTIVE   | INSERT_NEW_VERSION | CASE_11     | 2026-01-20 00:00:00 | 9999-12-31 23:59:59 | True           | True           |



**Dimensional Table `dim_person`**


| dp_key                                                                  | id                                   | first_name                               | last_name                                       | city                                    | email                                                      | dp_ts_from                                             | dp_ts_to                                                | dp_is_active                              | dp_is_latest                              | dp_created_at                                          | dp_replaced_at                                          |
|-------------------------------------------------------------------------|--------------------------------------|------------------------------------------|-------------------------------------------------|-----------------------------------------|------------------------------------------------------------|--------------------------------------------------------|---------------------------------------------------------|-------------------------------------------|-------------------------------------------|--------------------------------------------------------|---------------------------------------------------------|
| 4e7aeb65-70c4-43a9-8e85-7d9f8af9f542                                    | 1                                    | Alice                                    | Meyer                                           | Zurich                                  | alice.meyer@example.com                                    | 2026-01-01 00:00:00                                    | 2026-01-04 23:59:59                                     | False                                     | False                                     | 2026-01-02 00:00:00                                    | 2026-01-06 00:00:00                                     |
| d3ec9197-1256-4101-a9d9-040019ad8c66                                    | 1                                    | Alice                                    | Meyer                                           | Bern                                    | alice.meyer@example.com                                    | 2026-01-05 00:00:00                                    | 2026-01-09 23:59:59                                     | False                                     | False                                     | 2026-01-06 00:00:00                                    | 2026-01-11 00:00:00                                     |
| 72aad207-cab3-423a-a2da-63c87386e48c                                    | 1                                    | Alice                                    | Meyer                                           | Bern                                    | alice.meyer@newmail.com                                    | 2026-01-10 00:00:00                                    | <span style='color: orange;'>2026-01-19 23:59:59</span> | <span style='color: orange;'>False</span> | <span style='color: orange;'>False</span> | 2026-01-11 00:00:00                                    | <span style='color: orange;'>2026-01-21 00:00:00</span> |
| <span style='color: green;'>aa9f32c2-6c72-4dc6-a53b-70a1532ce7f5</span> | <span style='color: green;'>1</span> | <span style='color: green;'>Alice</span> | <span style='color: green;'>Müller-Meyer</span> | <span style='color: green;'>Bern</span> | <span style='color: green;'>alice.meyer@newmail.com</span> | <span style='color: green;'>2026-01-20 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  | <span style='color: green;'>True</span>   | <span style='color: green;'>True</span>   | <span style='color: green;'>2026-01-21 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  |
| 8766f706-c9d5-4b39-9a1b-deeae1e99cf7                                    | 2                                    | Bob                                      | Keller                                          | Bern                                    | bob.keller@example.com                                     | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                     | True                                      | True                                      | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                     |

_the following columns where excluded from the result: `record_hash, dp_load_timestamp, change_type`_

