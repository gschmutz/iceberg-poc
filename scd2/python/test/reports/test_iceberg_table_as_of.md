# Testing Timetravel

This test validates an SELECT ... FOR VERSION AS OF operation on an existing Iceberg table.
## Test Step 1
### Perform Test
Select all the latest data. Even though Bob has been deleted it will still be shown because we are selecting the latest records as of today.


`
        SELECT * 
        FROM iceberg_hive.default.raw_person
        ORDER BY id
        `



**Dimensional Table `dim_person`**


|   id | first_name   | last_name   | city   | email                    | status   | dp_exported_at      |
|------|--------------|-------------|--------|--------------------------|----------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@newcorp.com  | ACTIVE   | 2026-01-01 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-01 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-01 00:00:00 |

### Perform Test
Select all the latest data. Even though Bob has been deleted it will still be shown because we are selecting the latest records as of today.


`
        SELECT * 
        FROM iceberg_hive.default.raw_person
        FOR VERSION AS OF 372838778211275934
        ORDER BY id
        `



**Dimensional Table `dim_person`**


|   id | first_name   | last_name   | city   | email                    | status   | dp_exported_at      |
|------|--------------|-------------|--------|--------------------------|----------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-01 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-01 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-01 00:00:00 |

