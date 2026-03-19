# Testing for valid data at a given at a given timestamp

This test validates a single SELECT operation for data valid at a timestamp 2026-01-03 00:00:00
### Perform Preparation


**Raw Table `raw_person`**


|   id | first_name   | last_name   | city   | email                   | status   | dp_ts_from          | dp_loaded_at        |
|------|--------------|-------------|--------|-------------------------|----------|---------------------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com  | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    1 | Alice        | Meyer       | Bern   | alice.meyer@example.com | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com  | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |



**Dimensional Table `dim_person`**


| dp_key                               |   id | first_name   | last_name   | city   | email                   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   | dp_created_at       | dp_replaced_at      |
|--------------------------------------|------|--------------|-------------|--------|-------------------------|---------------------|---------------------|----------------|----------------|---------------------|---------------------|
| 3b7c55c2-cfad-4510-bbd1-f5ba3f67da0f |    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com | 2026-01-01 00:00:00 | 2026-01-04 23:59:59 | False          | False          | 2026-01-02 00:00:00 | 2026-01-06 00:00:00 |
| a634517b-642c-4f13-bcc7-310e82d6829e |    1 | Alice        | Meyer       | Bern   | alice.meyer@example.com | 2026-01-05 00:00:00 | 9999-12-31 23:59:59 | True           | True           | 2026-01-06 00:00:00 | 9999-12-31 23:59:59 |
| bcc0279c-318a-4306-9e98-1dd3ee944f30 |    2 | Bob          | Keller      | Bern   | bob.keller@example.com  | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           | 2026-01-02 00:00:00 | 9999-12-31 23:59:59 |

_the following columns where excluded from the result: `record_hash, dp_load_timestamp, change_type`_

### Perform Test
Select data valid at 2026-01-03 00:00:00. As we are selecting back in time the old version of Alice is shown where she lived in Zurich.


`
        SELECT id, first_name, last_name, city, email,
                dp_ts_from, dp_ts_to, dp_is_active, dp_is_latest,
                dp_created_at, dp_replaced_at,
                record_hash  
        FROM iceberg_hive.default.dim_person
        WHERE TIMESTAMP '2026-01-05 00:00:00' - INTERVAL '2' DAY BETWEEN dp_ts_from AND dp_ts_to
        ORDER BY id
        `



**Dimensional Table `dim_person`**


|   id | first_name   | last_name   | city   | email                   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   | dp_created_at       | dp_replaced_at      | record_hash                                                      |
|------|--------------|-------------|--------|-------------------------|---------------------|---------------------|----------------|----------------|---------------------|---------------------|------------------------------------------------------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com | 2026-01-01 00:00:00 | 2026-01-04 23:59:59 | False          | False          | 2026-01-02 00:00:00 | 2026-01-06 00:00:00 | 00B9A7122065F01BE7FD23C6FB962AEE6DE3B84D0BA50409DC26FC5A150FBDC8 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com  | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           | 2026-01-02 00:00:00 | 9999-12-31 23:59:59 | D28A23C8422275E006FCF3D86AA51CF4E058FB495B8E48560FC9BF7BCC019B40 |

