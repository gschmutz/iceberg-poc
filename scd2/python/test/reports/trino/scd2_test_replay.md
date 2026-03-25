# Testing Multiple Operations with a replay

This test validates multiple operations on one entity over time producing many versions followed by a replay of these operations. This proves that the SCD2 operations are idempotent, so that the exact same result as before the replay is still in place.
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


| dp_key                               |   id | first_name   | last_name    | city   | email                   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   | dp_created_at       | dp_replaced_at      |
|--------------------------------------|------|--------------|--------------|--------|-------------------------|---------------------|---------------------|----------------|----------------|---------------------|---------------------|
| 019c5f3b-b6f3-4fe7-939e-95f0a7a793d2 |    1 | Alice        | Meyer        | Zurich | alice.meyer@example.com | 2026-01-01 00:00:00 | 2026-01-04 23:59:59 | False          | False          | 2026-01-02 00:00:00 | 2026-01-06 00:00:00 |
| 26b0f5e9-6467-4e73-8e70-a867730e8c31 |    1 | Alice        | Meyer        | Bern   | alice.meyer@example.com | 2026-01-05 00:00:00 | 2026-01-09 23:59:59 | False          | False          | 2026-01-06 00:00:00 | 2026-01-11 00:00:00 |
| 7e3f3347-417b-41f9-bbe8-2d88de71907d |    1 | Alice        | Meyer        | Bern   | alice.meyer@newmail.com | 2026-01-10 00:00:00 | 2026-01-19 23:59:59 | False          | False          | 2026-01-11 00:00:00 | 2026-01-21 00:00:00 |
| eeb914c6-7b0b-4d71-8eee-087b399ccd06 |    1 | Alice        | Müller-Meyer | Bern   | alice.meyer@newmail.com | 2026-01-20 00:00:00 | 9999-12-31 23:59:59 | True           | True           | 2026-01-21 00:00:00 | 9999-12-31 23:59:59 |
| 719d0f41-05aa-4ce7-826c-016c7143a6c1 |    2 | Bob          | Keller       | Bern   | bob.keller@example.com  | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           | 2026-01-02 00:00:00 | 9999-12-31 23:59:59 |

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


| merge_key   | dp_key   | id   | first_name   | last_name   | city   | email   | record_hash   | load_ts   | status   | operation_type   | case_name   | dp_ts_from   | dp_ts_to   | dp_is_active   | dp_is_latest   |
|-------------|----------|------|--------------|-------------|--------|---------|---------------|-----------|----------|------------------|-------------|--------------|------------|----------------|----------------|



**Input to Merge**


| merge_key   | dp_key   | id   | first_name   | last_name   | city   | email   | record_hash   | load_ts   | status   | operation_type   | case_name   | dp_ts_from   | dp_ts_to   | dp_is_active   | dp_is_latest   |
|-------------|----------|------|--------------|-------------|--------|---------|---------------|-----------|----------|------------------|-------------|--------------|------------|----------------|----------------|



**Input to Merge**


| merge_key   | dp_key   | id   | first_name   | last_name   | city   | email   | record_hash   | load_ts   | status   | operation_type   | case_name   | dp_ts_from   | dp_ts_to   | dp_is_active   | dp_is_latest   |
|-------------|----------|------|--------------|-------------|--------|---------|---------------|-----------|----------|------------------|-------------|--------------|------------|----------------|----------------|



**Input to Merge**


| merge_key   | dp_key   | id   | first_name   | last_name   | city   | email   | record_hash   | load_ts   | status   | operation_type   | case_name   | dp_ts_from   | dp_ts_to   | dp_is_active   | dp_is_latest   |
|-------------|----------|------|--------------|-------------|--------|---------|---------------|-----------|----------|------------------|-------------|--------------|------------|----------------|----------------|



**Dimensional Table `dim_person`**


| dp_key                               |   id | first_name   | last_name    | city   | email                   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   | dp_created_at       | dp_replaced_at      |
|--------------------------------------|------|--------------|--------------|--------|-------------------------|---------------------|---------------------|----------------|----------------|---------------------|---------------------|
| 019c5f3b-b6f3-4fe7-939e-95f0a7a793d2 |    1 | Alice        | Meyer        | Zurich | alice.meyer@example.com | 2026-01-01 00:00:00 | 2026-01-04 23:59:59 | False          | False          | 2026-01-02 00:00:00 | 2026-01-06 00:00:00 |
| 26b0f5e9-6467-4e73-8e70-a867730e8c31 |    1 | Alice        | Meyer        | Bern   | alice.meyer@example.com | 2026-01-05 00:00:00 | 2026-01-09 23:59:59 | False          | False          | 2026-01-06 00:00:00 | 2026-01-11 00:00:00 |
| 7e3f3347-417b-41f9-bbe8-2d88de71907d |    1 | Alice        | Meyer        | Bern   | alice.meyer@newmail.com | 2026-01-10 00:00:00 | 2026-01-19 23:59:59 | False          | False          | 2026-01-11 00:00:00 | 2026-01-21 00:00:00 |
| eeb914c6-7b0b-4d71-8eee-087b399ccd06 |    1 | Alice        | Müller-Meyer | Bern   | alice.meyer@newmail.com | 2026-01-20 00:00:00 | 9999-12-31 23:59:59 | True           | True           | 2026-01-21 00:00:00 | 9999-12-31 23:59:59 |
| 719d0f41-05aa-4ce7-826c-016c7143a6c1 |    2 | Bob          | Keller       | Bern   | bob.keller@example.com  | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           | 2026-01-02 00:00:00 | 9999-12-31 23:59:59 |

_the following columns where excluded from the result: `record_hash, dp_load_timestamp, change_type`_

