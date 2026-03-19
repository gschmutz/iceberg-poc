# Testing Timetravel

This test validates an SELECT ... FOR VERSION AS OF operation on an existing Iceberg table.
## Test Step 1
### Perform Test
Select all the latest data. Even though Bob has been deleted it will still be shown because we are selecting the latest records as of today.


`
        SELECT * 
        FROM default.raw_person
        ORDER BY id
        `



**Dimensional Table `dim_person`**


| id   | first_name   | last_name   | city   | email   | status   | dp_ts_from   | dp_loaded_at   |
|------|--------------|-------------|--------|---------|----------|--------------|----------------|

### Assertion Error

```
DataFrame are different

DataFrame shape mismatch
[left]:  (0, 8)
[right]: (3, 8)
```

### Perform Test
Select all the latest data. Even though Bob has been deleted it will still be shown because we are selecting the latest records as of today.


`
        SELECT * 
        FROM default.raw_person
        FOR VERSION AS OF 7804853073982039846
        ORDER BY id
        `



**Dimensional Table `dim_person`**


|   id | first_name   | last_name   | city   | email                    | status   | dp_ts_from          | dp_loaded_at        |
|------|--------------|-------------|--------|--------------------------|----------|---------------------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@newcorp.com  | ACTIVE   | 2025-12-31 23:00:00 | 2025-12-31 23:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2025-12-31 23:00:00 | 2025-12-31 23:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2025-12-31 23:00:00 | 2025-12-31 23:00:00 |

### Assertion Error

```
DataFrame.iloc[:, 4] (column name="email") are different

DataFrame.iloc[:, 4] (column name="email") values are different (33.33333 %)
[index]: [0, 1, 2]
[left]:  [alice.meyer@newcorp.com, bob.keller@example.com, clara.schmid@example.com]
[right]: [alice.meyer@example.com, bob.keller@example.com, clara.schmid@example.com]
At positional index 0, first diff: alice.meyer@newcorp.com != alice.meyer@example.com
```

