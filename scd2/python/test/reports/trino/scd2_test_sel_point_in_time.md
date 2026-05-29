# Testing for valid data at a given at a given timestamp

This test validates a single SELECT operation for data valid at a timestamp 2026-01-03 00:00:00


 * **Strategy:** `trino`
 * **Last Run:** `2026-05-29 13:01:06`
### Perform Preparation


**Raw Table `raw_person`**


|   id | first_name   | last_name   | city   | email                   | status   | dp_ts_from          | dp_loaded_at        |
|------|--------------|-------------|--------|-------------------------|----------|---------------------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com  | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    1 | Alice        | Meyer       | Bern   | alice.meyer@example.com | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com  | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |



**Dimensional Table `dim_person`**


| dp_record_id                         |   id | first_name   | last_name   | city   | email                   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   | dp_load_ts          | dp_replace_ts       |
|--------------------------------------|------|--------------|-------------|--------|-------------------------|---------------------|---------------------|----------------|----------------|---------------------|---------------------|
| c895c3d3-438a-4eb7-8216-147635ae610f |    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com | 2026-01-01 00:00:00 | 2026-01-04 23:59:59 | False          | False          | 2026-01-02 00:00:00 | 2026-01-06 00:00:00 |
| 963fd8b2-5eae-40a4-8e8c-c8582f698222 |    1 | Alice        | Meyer       | Bern   | alice.meyer@example.com | 2026-01-05 00:00:00 | 9999-12-31 23:59:59 | True           | True           | 2026-01-06 00:00:00 | 9999-12-31 23:59:59 |
| df35c39a-f02d-4746-b4d8-9b86c9ad879a |    2 | Bob          | Keller      | Bern   | bob.keller@example.com  | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           | 2026-01-02 00:00:00 | 9999-12-31 23:59:59 |

_the following columns where excluded from the result: `dp_record_hash`_

### Perform Test
Select data valid at 2026-01-03 00:00:00. As we are selecting back in time the old version of Alice is shown where she lived in Zurich.


`
        SELECT id, first_name, last_name, city, email,
                dp_ts_from, dp_ts_to, dp_is_active, dp_is_latest,
                dp_load_ts, dp_replace_ts,
                dp_record_hash  
        FROM iceberg_hive.default.dim_person
        WHERE TIMESTAMP '2026-01-05 00:00:00' - INTERVAL '2' DAY BETWEEN dp_ts_from AND dp_ts_to
        ORDER BY id
        `



**Dimensional Table `dim_person`**


|   id | first_name   | last_name   | city   | email                   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   | dp_load_ts          | dp_replace_ts       | dp_record_hash                                                   |
|------|--------------|-------------|--------|-------------------------|---------------------|---------------------|----------------|----------------|---------------------|---------------------|------------------------------------------------------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com | 2026-01-01 00:00:00 | 2026-01-04 23:59:59 | False          | False          | 2026-01-02 00:00:00 | 2026-01-06 00:00:00 | F244BC679F09400F6966D6472E66C079A59943FEC38344C4306149D8034ED570 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com  | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           | 2026-01-02 00:00:00 | 9999-12-31 23:59:59 | E5181C5926D5185F99D4654A7F15E2476045F4808F22C5924E128B87DEB6F93F |

