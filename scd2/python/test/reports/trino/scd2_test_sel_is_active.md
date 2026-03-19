# Testing for valid data at a given at a given timestamp

This test validates a single SELECT operation for data valid at a timestamp 2026-01-03 00:00:00
Performing two batch inserts with load timestamps 2026-01-01 00:00:00 and 2026-01-05 00:00:00 and merge them into the dimension table. The second batch contains an update for Bob (status changes to INACTIVE) and a new record for Clara.
### Perform Preparation


**Raw Table `raw_person`**


|   id | first_name   | last_name   | city   | email                    | status   | dp_ts_from          | dp_loaded_at        |
|------|--------------|-------------|--------|--------------------------|----------|---------------------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    1 | Alice        | Meyer       | Bern   | alice.meyer@example.com  | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | INACTIVE | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |



**Dimensional Table `dim_person`**


| dp_key                               |   id | first_name   | last_name   | city   | email                    | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   | dp_created_at       | dp_replaced_at      |
|--------------------------------------|------|--------------|-------------|--------|--------------------------|---------------------|---------------------|----------------|----------------|---------------------|---------------------|
| 03151cea-6f0a-4795-af4d-782091f9ee3c |    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | 2026-01-01 00:00:00 | 2026-01-04 23:59:59 | False          | False          | 2026-01-02 00:00:00 | 2026-01-06 00:00:00 |
| 1504702d-a671-4b98-b55a-1ff0881eda27 |    1 | Alice        | Meyer       | Bern   | alice.meyer@example.com  | 2026-01-05 00:00:00 | 9999-12-31 23:59:59 | True           | True           | 2026-01-06 00:00:00 | 9999-12-31 23:59:59 |
| 25f1fccf-1797-4fb8-b23c-09c4a73a4cf5 |    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | 2026-01-01 00:00:00 | 2026-01-04 23:59:59 | False          | True           | 2026-01-02 00:00:00 | 2026-01-06 00:00:00 |
| 91d7e71a-6e5c-4c37-870d-f69b0237b62c |    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | 2026-01-05 00:00:00 | 9999-12-31 23:59:59 | True           | True           | 2026-01-06 00:00:00 | 9999-12-31 23:59:59 |

_the following columns where excluded from the result: `record_hash, dp_load_timestamp, change_type`_

### Perform Test
Select all the active data. Because Bob has been deleted at 2026-01-05 00:00:00 it will no longer be shown when selecting only ACTIVE records as of today.


`
        SELECT id, first_name, last_name, city, email,
                dp_ts_from, dp_ts_to, dp_is_active, dp_is_latest,
                dp_created_at, dp_replaced_at,
                record_hash 
        FROM iceberg_hive.default.dim_person
        WHERE dp_is_active = TRUE
        ORDER BY id
        `



**Dimensional Table `dim_person`**


|   id | first_name   | last_name   | city   | email                    | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   | dp_created_at       | dp_replaced_at      | record_hash                                                      |
|------|--------------|-------------|--------|--------------------------|---------------------|---------------------|----------------|----------------|---------------------|---------------------|------------------------------------------------------------------|
|    1 | Alice        | Meyer       | Bern   | alice.meyer@example.com  | 2026-01-05 00:00:00 | 9999-12-31 23:59:59 | True           | True           | 2026-01-06 00:00:00 | 9999-12-31 23:59:59 | 6449C8A21EC1B7B2BD4891618CF5853B27A97968D41570EE3CD34617BDBBD7BD |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | 2026-01-05 00:00:00 | 9999-12-31 23:59:59 | True           | True           | 2026-01-06 00:00:00 | 9999-12-31 23:59:59 | 77C069EE2AA3730894A6E3319ADC455C203B6CC4D35B0B912C2FAADF3C687676 |

