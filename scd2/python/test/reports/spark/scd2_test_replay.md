# Testing Multiple Operations with a replay

This test validates multiple operations on one entity over time producing many versions followed by a replay of these operations. This proves that the SCD2 operations are idempotent, so that the exact same result as before the replay is still in place.


 * **Strategy:** `spark`
 * **Last Run:** `2026-05-07 08:43:13`
### Perform Preparation


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



**Dimensional Table `dim_person`**


| dp_record_id                         |   id | first_name   | last_name    | city   | email                   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   | dp_load_ts          | dp_replace_ts       |
|--------------------------------------|------|--------------|--------------|--------|-------------------------|---------------------|---------------------|----------------|----------------|---------------------|---------------------|
| b81bad8f-374b-4749-ac94-a59875da466e |    1 | Alice        | Meyer        | Zurich | alice.meyer@example.com | 2026-01-01 00:00:00 | 2026-01-04 23:59:59 | False          | False          | 2026-01-02 00:00:00 | 2026-01-06 00:00:00 |
| b4fdc13a-f0f8-4c3b-a44b-6516b158a900 |    1 | Alice        | Meyer        | Bern   | alice.meyer@example.com | 2026-01-05 00:00:00 | 2026-01-09 23:59:59 | False          | False          | 2026-01-06 00:00:00 | 2026-01-11 00:00:00 |
| e8d735de-62fa-47b8-a2ed-ab3934fb38f0 |    1 | Alice        | Meyer        | Bern   | alice.meyer@newmail.com | 2026-01-10 00:00:00 | 2026-01-19 23:59:59 | False          | False          | 2026-01-11 00:00:00 | 2026-01-21 00:00:00 |
| 2a7942f2-6f7b-4cd2-993c-f8dab20de31e |    1 | Alice        | Müller-Meyer | Bern   | alice.meyer@newmail.com | 2026-01-20 00:00:00 | 9999-12-31 23:59:59 | True           | True           | 2026-01-21 00:00:00 | 9999-12-31 23:59:59 |
| 3f3dce59-17b8-4b94-9d8a-c96bbe60e0b4 |    2 | Bob          | Keller       | Bern   | bob.keller@example.com  | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           | 2026-01-02 00:00:00 | 9999-12-31 23:59:59 |

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


| merge_record_id   | dp_record_id   | id   | first_name   | last_name   | city   | email   | dp_record_hash   | dp_del_flag   | operation_type   | case_name   | dp_ts_from   | dp_ts_to   | dp_is_active   | dp_is_latest   |
|-------------------|----------------|------|--------------|-------------|--------|---------|------------------|---------------|------------------|-------------|--------------|------------|----------------|----------------|



**Input to Merge**


| merge_record_id   | dp_record_id   | id   | first_name   | last_name   | city   | email   | dp_record_hash   | dp_del_flag   | operation_type   | case_name   | dp_ts_from   | dp_ts_to   | dp_is_active   | dp_is_latest   |
|-------------------|----------------|------|--------------|-------------|--------|---------|------------------|---------------|------------------|-------------|--------------|------------|----------------|----------------|



**Input to Merge**


| merge_record_id   | dp_record_id   | id   | first_name   | last_name   | city   | email   | dp_record_hash   | dp_del_flag   | operation_type   | case_name   | dp_ts_from   | dp_ts_to   | dp_is_active   | dp_is_latest   |
|-------------------|----------------|------|--------------|-------------|--------|---------|------------------|---------------|------------------|-------------|--------------|------------|----------------|----------------|



**Input to Merge**


| merge_record_id   | dp_record_id   | id   | first_name   | last_name   | city   | email   | dp_record_hash   | dp_del_flag   | operation_type   | case_name   | dp_ts_from   | dp_ts_to   | dp_is_active   | dp_is_latest   |
|-------------------|----------------|------|--------------|-------------|--------|---------|------------------|---------------|------------------|-------------|--------------|------------|----------------|----------------|



**Dimensional Table `dim_person`**


| dp_record_id                         |   id | first_name   | last_name    | city   | email                   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   | dp_load_ts          | dp_replace_ts       |
|--------------------------------------|------|--------------|--------------|--------|-------------------------|---------------------|---------------------|----------------|----------------|---------------------|---------------------|
| b81bad8f-374b-4749-ac94-a59875da466e |    1 | Alice        | Meyer        | Zurich | alice.meyer@example.com | 2026-01-01 00:00:00 | 2026-01-04 23:59:59 | False          | False          | 2026-01-02 00:00:00 | 2026-01-06 00:00:00 |
| b4fdc13a-f0f8-4c3b-a44b-6516b158a900 |    1 | Alice        | Meyer        | Bern   | alice.meyer@example.com | 2026-01-05 00:00:00 | 2026-01-09 23:59:59 | False          | False          | 2026-01-06 00:00:00 | 2026-01-11 00:00:00 |
| e8d735de-62fa-47b8-a2ed-ab3934fb38f0 |    1 | Alice        | Meyer        | Bern   | alice.meyer@newmail.com | 2026-01-10 00:00:00 | 2026-01-19 23:59:59 | False          | False          | 2026-01-11 00:00:00 | 2026-01-21 00:00:00 |
| 2a7942f2-6f7b-4cd2-993c-f8dab20de31e |    1 | Alice        | Müller-Meyer | Bern   | alice.meyer@newmail.com | 2026-01-20 00:00:00 | 9999-12-31 23:59:59 | True           | True           | 2026-01-21 00:00:00 | 9999-12-31 23:59:59 |
| 3f3dce59-17b8-4b94-9d8a-c96bbe60e0b4 |    2 | Bob          | Keller       | Bern   | bob.keller@example.com  | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           | 2026-01-02 00:00:00 | 9999-12-31 23:59:59 |

_the following columns where excluded from the result: `dp_record_hash`_

