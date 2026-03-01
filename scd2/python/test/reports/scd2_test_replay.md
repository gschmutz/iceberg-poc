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
| e5804dcd-db83-4afd-89fa-830322a8cea7 |    1 | Alice        | Meyer        | Zurich | alice.meyer@example.com | 2026-01-01 00:00:00 | 2026-01-04 23:59:59 | False          | False          | 2026-01-02 00:00:00 | 2026-01-06 00:00:00 |
| b7887c13-9c89-4bdc-872d-32faeb54c06e |    1 | Alice        | Meyer        | Bern   | alice.meyer@example.com | 2026-01-05 00:00:00 | 2026-01-09 23:59:59 | False          | False          | 2026-01-06 00:00:00 | 2026-01-11 00:00:00 |
| f5a4f3c9-7099-4599-aa77-2d6c0f020e9f |    1 | Alice        | Meyer        | Bern   | alice.meyer@newmail.com | 2026-01-10 00:00:00 | 2026-01-19 23:59:59 | False          | False          | 2026-01-11 00:00:00 | 2026-01-21 00:00:00 |
| 7abbb59f-d5f0-4548-a239-53b22b3fa157 |    1 | Alice        | Müller-Meyer | Bern   | alice.meyer@newmail.com | 2026-01-20 00:00:00 | 9999-12-31 23:59:59 | True           | True           | 2026-01-21 00:00:00 | 9999-12-31 23:59:59 |
| da4dee75-9914-4bfa-a746-73e5cda87b71 |    2 | Bob          | Keller       | Bern   | bob.keller@example.com  | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           | 2026-01-02 00:00:00 | 9999-12-31 23:59:59 |

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


| merge_key   | dp_key   | id   | first_name   | last_name   | city   | email   | src_dp_ts_from   | load_ts   | status   | change_classification   | operation_type   | tgt_dp_ts_from   | tgt_dp_ts_to   | prev_dp_ts_from   | prev_dp_ts_to   | succ_dp_ts_from   | succ_dp_ts_to   |
|-------------|----------|------|--------------|-------------|--------|---------|------------------|-----------|----------|-------------------------|------------------|------------------|----------------|-------------------|-----------------|-------------------|-----------------|



**Input to Merge**


| merge_key   | dp_key   | id   | first_name   | last_name   | city   | email   | src_dp_ts_from   | load_ts   | status   | change_classification   | operation_type   | tgt_dp_ts_from   | tgt_dp_ts_to   | prev_dp_ts_from   | prev_dp_ts_to   | succ_dp_ts_from   | succ_dp_ts_to   |
|-------------|----------|------|--------------|-------------|--------|---------|------------------|-----------|----------|-------------------------|------------------|------------------|----------------|-------------------|-----------------|-------------------|-----------------|



**Input to Merge**


| merge_key   | dp_key   | id   | first_name   | last_name   | city   | email   | src_dp_ts_from   | load_ts   | status   | change_classification   | operation_type   | tgt_dp_ts_from   | tgt_dp_ts_to   | prev_dp_ts_from   | prev_dp_ts_to   | succ_dp_ts_from   | succ_dp_ts_to   |
|-------------|----------|------|--------------|-------------|--------|---------|------------------|-----------|----------|-------------------------|------------------|------------------|----------------|-------------------|-----------------|-------------------|-----------------|



**Input to Merge**


| merge_key   | dp_key   | id   | first_name   | last_name   | city   | email   | src_dp_ts_from   | load_ts   | status   | change_classification   | operation_type   | tgt_dp_ts_from   | tgt_dp_ts_to   | prev_dp_ts_from   | prev_dp_ts_to   | succ_dp_ts_from   | succ_dp_ts_to   |
|-------------|----------|------|--------------|-------------|--------|---------|------------------|-----------|----------|-------------------------|------------------|------------------|----------------|-------------------|-----------------|-------------------|-----------------|



**Dimensional Table `dim_person`**


| dp_key                               |   id | first_name   | last_name    | city   | email                   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   | dp_created_at       | dp_replaced_at      |
|--------------------------------------|------|--------------|--------------|--------|-------------------------|---------------------|---------------------|----------------|----------------|---------------------|---------------------|
| e5804dcd-db83-4afd-89fa-830322a8cea7 |    1 | Alice        | Meyer        | Zurich | alice.meyer@example.com | 2026-01-01 00:00:00 | 2026-01-04 23:59:59 | False          | False          | 2026-01-02 00:00:00 | 2026-01-06 00:00:00 |
| b7887c13-9c89-4bdc-872d-32faeb54c06e |    1 | Alice        | Meyer        | Bern   | alice.meyer@example.com | 2026-01-05 00:00:00 | 2026-01-09 23:59:59 | False          | False          | 2026-01-06 00:00:00 | 2026-01-11 00:00:00 |
| f5a4f3c9-7099-4599-aa77-2d6c0f020e9f |    1 | Alice        | Meyer        | Bern   | alice.meyer@newmail.com | 2026-01-10 00:00:00 | 2026-01-19 23:59:59 | False          | False          | 2026-01-11 00:00:00 | 2026-01-21 00:00:00 |
| 7abbb59f-d5f0-4548-a239-53b22b3fa157 |    1 | Alice        | Müller-Meyer | Bern   | alice.meyer@newmail.com | 2026-01-20 00:00:00 | 9999-12-31 23:59:59 | True           | True           | 2026-01-21 00:00:00 | 9999-12-31 23:59:59 |
| da4dee75-9914-4bfa-a746-73e5cda87b71 |    2 | Bob          | Keller       | Bern   | bob.keller@example.com  | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           | 2026-01-02 00:00:00 | 9999-12-31 23:59:59 |

_the following columns where excluded from the result: `record_hash, dp_load_timestamp, change_type`_

