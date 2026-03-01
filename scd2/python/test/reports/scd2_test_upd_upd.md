# Testing Multiple Update Operations on same entity but different fields

This test validates multiple UPDATE operations on one entity over time producing many versions.
## Test Step 1
Insert 2 entities into raw table and perform initial SCD2 merge.


**Raw Table `raw_person`**


|   id | first_name   | last_name   | city   | email                   | status   | dp_ts_from          | dp_loaded_at        |
|------|--------------|-------------|--------|-------------------------|----------|---------------------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com  | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |



**Input to Merge**


| merge_key   | dp_key   |   id | first_name   | last_name   | city   | email                   | src_dp_ts_from      | load_ts             | status   | change_classification   | operation_type   | tgt_dp_ts_from      | tgt_dp_ts_to        | prev_dp_ts_from   | prev_dp_ts_to   | succ_dp_ts_from   | succ_dp_ts_to   |
|-------------|----------|------|--------------|-------------|--------|-------------------------|---------------------|---------------------|----------|-------------------------|------------------|---------------------|---------------------|-------------------|-----------------|-------------------|-----------------|
|             |          |    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 | ACTIVE   | NEW                     | UPDATE_EXISTING  | 9999-12-31 23:59:59 | 9999-12-31 23:59:59 |                   |                 |                   |                 |
|             |          |    2 | Bob          | Keller      | Bern   | bob.keller@example.com  | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 | ACTIVE   | NEW                     | UPDATE_EXISTING  | 9999-12-31 23:59:59 | 9999-12-31 23:59:59 |                   |                 |                   |                 |



**Dimensional Table `dim_person`**


| dp_key                                                                  | id                                   | first_name                               | last_name                                 | city                                      | email                                                      | dp_ts_from                                             | dp_ts_to                                               | dp_is_active                            | dp_is_latest                            | dp_created_at                                          | dp_replaced_at                                         |
|-------------------------------------------------------------------------|--------------------------------------|------------------------------------------|-------------------------------------------|-------------------------------------------|------------------------------------------------------------|--------------------------------------------------------|--------------------------------------------------------|-----------------------------------------|-----------------------------------------|--------------------------------------------------------|--------------------------------------------------------|
| <span style='color: green;'>44278a9e-bcab-4fc8-a92c-65a9c9d5e051</span> | <span style='color: green;'>1</span> | <span style='color: green;'>Alice</span> | <span style='color: green;'>Meyer</span>  | <span style='color: green;'>Zurich</span> | <span style='color: green;'>alice.meyer@example.com</span> | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>bf183465-ec09-456d-90d1-e031bc2596cd</span> | <span style='color: green;'>2</span> | <span style='color: green;'>Bob</span>   | <span style='color: green;'>Keller</span> | <span style='color: green;'>Bern</span>   | <span style='color: green;'>bob.keller@example.com</span>  | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |

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


| merge_key                            | dp_key                               |   id | first_name   | last_name   | city   | email                   | src_dp_ts_from      | load_ts             | status   | change_classification   | operation_type     | tgt_dp_ts_from      | tgt_dp_ts_to        | prev_dp_ts_from   | prev_dp_ts_to   | succ_dp_ts_from   | succ_dp_ts_to   |
|--------------------------------------|--------------------------------------|------|--------------|-------------|--------|-------------------------|---------------------|---------------------|----------|-------------------------|--------------------|---------------------|---------------------|-------------------|-----------------|-------------------|-----------------|
| 44278a9e-bcab-4fc8-a92c-65a9c9d5e051 | 44278a9e-bcab-4fc8-a92c-65a9c9d5e051 |    1 | Alice        | Meyer       | Bern   | alice.meyer@example.com | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 | ACTIVE   | CHANGED                 | UPDATE_EXISTING    | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 |                   |                 |                   |                 |
|                                      | 44278a9e-bcab-4fc8-a92c-65a9c9d5e051 |    1 | Alice        | Meyer       | Bern   | alice.meyer@example.com | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 | ACTIVE   | CHANGED                 | INSERT_NEW_VERSION | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 |                   |                 |                   |                 |



**Dimensional Table `dim_person`**


| dp_key                                                                  | id                                   | first_name                               | last_name                                | city                                    | email                                                      | dp_ts_from                                             | dp_ts_to                                                | dp_is_active                              | dp_is_latest                              | dp_created_at                                          | dp_replaced_at                                          |
|-------------------------------------------------------------------------|--------------------------------------|------------------------------------------|------------------------------------------|-----------------------------------------|------------------------------------------------------------|--------------------------------------------------------|---------------------------------------------------------|-------------------------------------------|-------------------------------------------|--------------------------------------------------------|---------------------------------------------------------|
| 44278a9e-bcab-4fc8-a92c-65a9c9d5e051                                    | 1                                    | Alice                                    | Meyer                                    | Zurich                                  | alice.meyer@example.com                                    | 2026-01-01 00:00:00                                    | <span style='color: orange;'>2026-01-04 23:59:59</span> | <span style='color: orange;'>False</span> | <span style='color: orange;'>False</span> | 2026-01-02 00:00:00                                    | <span style='color: orange;'>2026-01-06 00:00:00</span> |
| <span style='color: green;'>bc04b4c5-c0d6-42eb-adca-5805aa96b5b4</span> | <span style='color: green;'>1</span> | <span style='color: green;'>Alice</span> | <span style='color: green;'>Meyer</span> | <span style='color: green;'>Bern</span> | <span style='color: green;'>alice.meyer@example.com</span> | <span style='color: green;'>2026-01-05 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  | <span style='color: green;'>True</span>   | <span style='color: green;'>True</span>   | <span style='color: green;'>2026-01-06 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  |
| bf183465-ec09-456d-90d1-e031bc2596cd                                    | 2                                    | Bob                                      | Keller                                   | Bern                                    | bob.keller@example.com                                     | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                     | True                                      | True                                      | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                     |

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


| merge_key                            | dp_key                               |   id | first_name   | last_name   | city   | email                   | src_dp_ts_from      | load_ts             | status   | change_classification   | operation_type     | tgt_dp_ts_from      | tgt_dp_ts_to        | prev_dp_ts_from   | prev_dp_ts_to   | succ_dp_ts_from   | succ_dp_ts_to   |
|--------------------------------------|--------------------------------------|------|--------------|-------------|--------|-------------------------|---------------------|---------------------|----------|-------------------------|--------------------|---------------------|---------------------|-------------------|-----------------|-------------------|-----------------|
| bc04b4c5-c0d6-42eb-adca-5805aa96b5b4 | bc04b4c5-c0d6-42eb-adca-5805aa96b5b4 |    1 | Alice        | Meyer       | Bern   | alice.meyer@newmail.com | 2026-01-10 00:00:00 | 2026-01-10 00:00:00 | ACTIVE   | CHANGED                 | UPDATE_EXISTING    | 2026-01-05 00:00:00 | 9999-12-31 23:59:59 |                   |                 |                   |                 |
|                                      | bc04b4c5-c0d6-42eb-adca-5805aa96b5b4 |    1 | Alice        | Meyer       | Bern   | alice.meyer@newmail.com | 2026-01-10 00:00:00 | 2026-01-10 00:00:00 | ACTIVE   | CHANGED                 | INSERT_NEW_VERSION | 2026-01-05 00:00:00 | 9999-12-31 23:59:59 |                   |                 |                   |                 |



**Dimensional Table `dim_person`**


| dp_key                                                                  | id                                   | first_name                               | last_name                                | city                                    | email                                                      | dp_ts_from                                             | dp_ts_to                                                | dp_is_active                              | dp_is_latest                              | dp_created_at                                          | dp_replaced_at                                          |
|-------------------------------------------------------------------------|--------------------------------------|------------------------------------------|------------------------------------------|-----------------------------------------|------------------------------------------------------------|--------------------------------------------------------|---------------------------------------------------------|-------------------------------------------|-------------------------------------------|--------------------------------------------------------|---------------------------------------------------------|
| 44278a9e-bcab-4fc8-a92c-65a9c9d5e051                                    | 1                                    | Alice                                    | Meyer                                    | Zurich                                  | alice.meyer@example.com                                    | 2026-01-01 00:00:00                                    | 2026-01-04 23:59:59                                     | False                                     | False                                     | 2026-01-02 00:00:00                                    | 2026-01-06 00:00:00                                     |
| bc04b4c5-c0d6-42eb-adca-5805aa96b5b4                                    | 1                                    | Alice                                    | Meyer                                    | Bern                                    | alice.meyer@example.com                                    | 2026-01-05 00:00:00                                    | <span style='color: orange;'>2026-01-09 23:59:59</span> | <span style='color: orange;'>False</span> | <span style='color: orange;'>False</span> | 2026-01-06 00:00:00                                    | <span style='color: orange;'>2026-01-11 00:00:00</span> |
| <span style='color: green;'>5b8dafb7-8cfa-4540-9250-543a62463b6c</span> | <span style='color: green;'>1</span> | <span style='color: green;'>Alice</span> | <span style='color: green;'>Meyer</span> | <span style='color: green;'>Bern</span> | <span style='color: green;'>alice.meyer@newmail.com</span> | <span style='color: green;'>2026-01-10 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  | <span style='color: green;'>True</span>   | <span style='color: green;'>True</span>   | <span style='color: green;'>2026-01-11 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  |
| bf183465-ec09-456d-90d1-e031bc2596cd                                    | 2                                    | Bob                                      | Keller                                   | Bern                                    | bob.keller@example.com                                     | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                     | True                                      | True                                      | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                     |

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


| merge_key                            | dp_key                               |   id | first_name   | last_name    | city   | email                   | src_dp_ts_from      | load_ts             | status   | change_classification   | operation_type     | tgt_dp_ts_from      | tgt_dp_ts_to        | prev_dp_ts_from   | prev_dp_ts_to   | succ_dp_ts_from   | succ_dp_ts_to   |
|--------------------------------------|--------------------------------------|------|--------------|--------------|--------|-------------------------|---------------------|---------------------|----------|-------------------------|--------------------|---------------------|---------------------|-------------------|-----------------|-------------------|-----------------|
| 5b8dafb7-8cfa-4540-9250-543a62463b6c | 5b8dafb7-8cfa-4540-9250-543a62463b6c |    1 | Alice        | Müller-Meyer | Bern   | alice.meyer@newmail.com | 2026-01-20 00:00:00 | 2026-01-20 00:00:00 | ACTIVE   | CHANGED                 | UPDATE_EXISTING    | 2026-01-10 00:00:00 | 9999-12-31 23:59:59 |                   |                 |                   |                 |
|                                      | 5b8dafb7-8cfa-4540-9250-543a62463b6c |    1 | Alice        | Müller-Meyer | Bern   | alice.meyer@newmail.com | 2026-01-20 00:00:00 | 2026-01-20 00:00:00 | ACTIVE   | CHANGED                 | INSERT_NEW_VERSION | 2026-01-10 00:00:00 | 9999-12-31 23:59:59 |                   |                 |                   |                 |



**Dimensional Table `dim_person`**


| dp_key                                                                  | id                                   | first_name                               | last_name                                       | city                                    | email                                                      | dp_ts_from                                             | dp_ts_to                                                | dp_is_active                              | dp_is_latest                              | dp_created_at                                          | dp_replaced_at                                          |
|-------------------------------------------------------------------------|--------------------------------------|------------------------------------------|-------------------------------------------------|-----------------------------------------|------------------------------------------------------------|--------------------------------------------------------|---------------------------------------------------------|-------------------------------------------|-------------------------------------------|--------------------------------------------------------|---------------------------------------------------------|
| 44278a9e-bcab-4fc8-a92c-65a9c9d5e051                                    | 1                                    | Alice                                    | Meyer                                           | Zurich                                  | alice.meyer@example.com                                    | 2026-01-01 00:00:00                                    | 2026-01-04 23:59:59                                     | False                                     | False                                     | 2026-01-02 00:00:00                                    | 2026-01-06 00:00:00                                     |
| bc04b4c5-c0d6-42eb-adca-5805aa96b5b4                                    | 1                                    | Alice                                    | Meyer                                           | Bern                                    | alice.meyer@example.com                                    | 2026-01-05 00:00:00                                    | 2026-01-09 23:59:59                                     | False                                     | False                                     | 2026-01-06 00:00:00                                    | 2026-01-11 00:00:00                                     |
| 5b8dafb7-8cfa-4540-9250-543a62463b6c                                    | 1                                    | Alice                                    | Meyer                                           | Bern                                    | alice.meyer@newmail.com                                    | 2026-01-10 00:00:00                                    | <span style='color: orange;'>2026-01-19 23:59:59</span> | <span style='color: orange;'>False</span> | <span style='color: orange;'>False</span> | 2026-01-11 00:00:00                                    | <span style='color: orange;'>2026-01-21 00:00:00</span> |
| <span style='color: green;'>ba83885c-2125-445b-b2ee-9f0dcdd05b74</span> | <span style='color: green;'>1</span> | <span style='color: green;'>Alice</span> | <span style='color: green;'>Müller-Meyer</span> | <span style='color: green;'>Bern</span> | <span style='color: green;'>alice.meyer@newmail.com</span> | <span style='color: green;'>2026-01-20 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  | <span style='color: green;'>True</span>   | <span style='color: green;'>True</span>   | <span style='color: green;'>2026-01-21 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  |
| bf183465-ec09-456d-90d1-e031bc2596cd                                    | 2                                    | Bob                                      | Keller                                          | Bern                                    | bob.keller@example.com                                     | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                     | True                                      | True                                      | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                     |

_the following columns where excluded from the result: `record_hash, dp_load_timestamp, change_type`_

