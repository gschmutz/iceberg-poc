# Testing Multiple Operations with a replay

This test validates multiple operations on one entity over time producing many versions followed by a replay of these operations. This proves that the SCD2 operations are idempotent, so that the exact same result as before the replay is still in place.


 * **Strategy:** `trino`
 * **Last Run:** `2026-06-08 18:38:05`
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
| 76869c90-7e29-4e6c-8abb-94023e613d1f |    1 | Alice        | Meyer        | Zurich | alice.meyer@example.com | 2026-01-01 00:00:00 | 2026-01-04 23:59:59 | False          | False          | 2026-01-02 00:00:00 | 2026-01-06 00:00:00 |
| bc1c2092-054a-4bb9-8564-3aa7aa17eda7 |    1 | Alice        | Meyer        | Bern   | alice.meyer@example.com | 2026-01-05 00:00:00 | 2026-01-09 23:59:59 | False          | False          | 2026-01-06 00:00:00 | 2026-01-11 00:00:00 |
| 2a4e7a12-51d1-4a17-a55f-3ec247c16fe4 |    1 | Alice        | Meyer        | Bern   | alice.meyer@newmail.com | 2026-01-10 00:00:00 | 2026-01-19 23:59:59 | False          | False          | 2026-01-11 00:00:00 | 2026-01-21 00:00:00 |
| e3893c70-d061-40df-9c92-ab75269e4609 |    1 | Alice        | Müller-Meyer | Bern   | alice.meyer@newmail.com | 2026-01-20 00:00:00 | 9999-12-31 23:59:59 | True           | True           | 2026-01-21 00:00:00 | 9999-12-31 23:59:59 |
| b2d99cab-4a63-43a3-98cd-bb8c021fd745 |    2 | Bob          | Keller       | Bern   | bob.keller@example.com  | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           | 2026-01-02 00:00:00 | 9999-12-31 23:59:59 |

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
| 76869c90-7e29-4e6c-8abb-94023e613d1f |    1 | Alice        | Meyer        | Zurich | alice.meyer@example.com | 2026-01-01 00:00:00 | 2026-01-04 23:59:59 | False          | False          | 2026-01-02 00:00:00 | 2026-01-06 00:00:00 |
| bc1c2092-054a-4bb9-8564-3aa7aa17eda7 |    1 | Alice        | Meyer        | Bern   | alice.meyer@example.com | 2026-01-05 00:00:00 | 2026-01-09 23:59:59 | False          | False          | 2026-01-06 00:00:00 | 2026-01-11 00:00:00 |
| 2a4e7a12-51d1-4a17-a55f-3ec247c16fe4 |    1 | Alice        | Meyer        | Bern   | alice.meyer@newmail.com | 2026-01-10 00:00:00 | 2026-01-19 23:59:59 | False          | False          | 2026-01-11 00:00:00 | 2026-01-21 00:00:00 |
| e3893c70-d061-40df-9c92-ab75269e4609 |    1 | Alice        | Müller-Meyer | Bern   | alice.meyer@newmail.com | 2026-01-20 00:00:00 | 9999-12-31 23:59:59 | True           | True           | 2026-01-21 00:00:00 | 9999-12-31 23:59:59 |
| b2d99cab-4a63-43a3-98cd-bb8c021fd745 |    2 | Bob          | Keller       | Bern   | bob.keller@example.com  | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           | 2026-01-02 00:00:00 | 9999-12-31 23:59:59 |

_the following columns where excluded from the result: `dp_record_hash`_

