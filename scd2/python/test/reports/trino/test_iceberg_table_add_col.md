# Testing Add Column to existing Iceberg table

This test validates an ALTER TABLE ADD COLUMN operation on an existing Iceberg table.
 * **Strategy:** `trino`
 * **Last Run:** `2026-04-09 18:29:00`
## Test Step 1


**Table raw_person before ADD COLUMN**


|   id | first_name   | last_name   | city   | email                    | status   | dp_ts_from          | dp_loaded_at        |
|------|--------------|-------------|--------|--------------------------|----------|---------------------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |

Executing ADD COLUMN
### Perform Test
Select all the latest data. Even though Bob has been deleted it will still be shown because we are selecting the latest records as of today.


`
        SELECT * 
        FROM iceberg_hive.default.raw_person
        ORDER BY id
        `



**Dimensional Table `dim_person`**


|   id | first_name   | last_name   | city   | email                    | new_col   | status   | dp_ts_from          | dp_loaded_at        |
|------|--------------|-------------|--------|--------------------------|-----------|----------|---------------------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | New Value | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | New Value | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | New Value | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |

