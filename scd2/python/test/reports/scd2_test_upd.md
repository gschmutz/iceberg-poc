# Testing Update Operation

This test validates an UPDATE operation of one entity (with a new version) on a set of existing entities.
## Test Step 1
Insert 3 entities into raw table and perform initial SCD2 merge.
### Raw Table `raw_person`

|   id | first_name   | last_name   | city   | email                    | status   | dp_exported_at      |
|------|--------------|-------------|--------|--------------------------|----------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-01 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-01 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-01 00:00:00 |

### Input to Merge

|   merge_key |   id | first_name   | last_name   | city   | email                    | load_ts             | status   | change_classification   | operation_type   |
|-------------|------|--------------|-------------|--------|--------------------------|---------------------|----------|-------------------------|------------------|
|           1 |    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | 2026-01-01 00:00:00 | ACTIVE   | NEW                     | UPDATE_EXISTING  |
|           2 |    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | 2026-01-01 00:00:00 | ACTIVE   | NEW                     | UPDATE_EXISTING  |
|           3 |    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | 2026-01-01 00:00:00 | ACTIVE   | NEW                     | UPDATE_EXISTING  |

### Dimensional Table `dim_person`

| id                                   | first_name                               | last_name                                 | city                                      | email                                                       | dp_valid_from                                          | dp_valid_to                                            | dp_is_active                            | dp_is_latest                            | dp_created_at                                          | dp_replaced_at                                         |
|--------------------------------------|------------------------------------------|-------------------------------------------|-------------------------------------------|-------------------------------------------------------------|--------------------------------------------------------|--------------------------------------------------------|-----------------------------------------|-----------------------------------------|--------------------------------------------------------|--------------------------------------------------------|
| <span style='color: green;'>1</span> | <span style='color: green;'>Alice</span> | <span style='color: green;'>Meyer</span>  | <span style='color: green;'>Zurich</span> | <span style='color: green;'>alice.meyer@example.com</span>  | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>2</span> | <span style='color: green;'>Bob</span>   | <span style='color: green;'>Keller</span> | <span style='color: green;'>Bern</span>   | <span style='color: green;'>bob.keller@example.com</span>   | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>3</span> | <span style='color: green;'>Clara</span> | <span style='color: green;'>Schmid</span> | <span style='color: green;'>Basel</span>  | <span style='color: green;'>clara.schmid@example.com</span> | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |

_the following columns where excluded from the result: `record_hash, dp_load_timestamp, change_type`_

## Test Step 2
At 2026-01-05 00:00:00, update entity with `id=3` in raw table to INACTIVE (logical delete) and perform SCD2 merge.
### Raw Table `raw_person`

|   id | first_name   | last_name   | city   | email                    | status   | dp_exported_at      |
|------|--------------|-------------|--------|--------------------------|----------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-01 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-01 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-01 00:00:00 |
|    1 | Alice        | Meyer       | Bern   | alice.meyer@example.com  | ACTIVE   | 2026-01-05 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-05 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@newmail.com | ACTIVE   | 2026-01-05 00:00:00 |

### Input to Merge

|   merge_key |   id | first_name   | last_name   | city   | email                    | load_ts             | status   | change_classification   | operation_type     |
|-------------|------|--------------|-------------|--------|--------------------------|---------------------|----------|-------------------------|--------------------|
|           1 |    1 | Alice        | Meyer       | Bern   | alice.meyer@example.com  | 2026-01-05 00:00:00 | ACTIVE   | CHANGED                 | UPDATE_EXISTING    |
|           3 |    3 | Clara        | Schmid      | Basel  | clara.schmid@newmail.com | 2026-01-05 00:00:00 | ACTIVE   | CHANGED                 | UPDATE_EXISTING    |
|         nan |    1 | Alice        | Meyer       | Bern   | alice.meyer@example.com  | 2026-01-05 00:00:00 | ACTIVE   | CHANGED                 | INSERT_NEW_VERSION |
|         nan |    3 | Clara        | Schmid      | Basel  | clara.schmid@newmail.com | 2026-01-05 00:00:00 | ACTIVE   | CHANGED                 | INSERT_NEW_VERSION |

### Dimensional Table `dim_person`

| id                                   | first_name                               | last_name                                 | city                                     | email                                                       | dp_valid_from                                          | dp_valid_to                                             | dp_is_active                              | dp_is_latest                              | dp_created_at                                          | dp_replaced_at                                          |
|--------------------------------------|------------------------------------------|-------------------------------------------|------------------------------------------|-------------------------------------------------------------|--------------------------------------------------------|---------------------------------------------------------|-------------------------------------------|-------------------------------------------|--------------------------------------------------------|---------------------------------------------------------|
| 1                                    | Alice                                    | Meyer                                     | Zurich                                   | alice.meyer@example.com                                     | 2026-01-01 00:00:00                                    | <span style='color: orange;'>2026-01-04 23:59:59</span> | <span style='color: orange;'>False</span> | <span style='color: orange;'>False</span> | 2026-01-02 00:00:00                                    | <span style='color: orange;'>2026-01-06 00:00:00</span> |
| <span style='color: green;'>1</span> | <span style='color: green;'>Alice</span> | <span style='color: green;'>Meyer</span>  | <span style='color: green;'>Bern</span>  | <span style='color: green;'>alice.meyer@example.com</span>  | <span style='color: green;'>2026-01-05 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  | <span style='color: green;'>True</span>   | <span style='color: green;'>True</span>   | <span style='color: green;'>2026-01-06 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  |
| 2                                    | Bob                                      | Keller                                    | Bern                                     | bob.keller@example.com                                      | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                     | True                                      | True                                      | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                     |
| 3                                    | Clara                                    | Schmid                                    | Basel                                    | clara.schmid@example.com                                    | 2026-01-01 00:00:00                                    | <span style='color: orange;'>2026-01-04 23:59:59</span> | <span style='color: orange;'>False</span> | <span style='color: orange;'>False</span> | 2026-01-02 00:00:00                                    | <span style='color: orange;'>2026-01-06 00:00:00</span> |
| <span style='color: green;'>3</span> | <span style='color: green;'>Clara</span> | <span style='color: green;'>Schmid</span> | <span style='color: green;'>Basel</span> | <span style='color: green;'>clara.schmid@newmail.com</span> | <span style='color: green;'>2026-01-05 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  | <span style='color: green;'>True</span>   | <span style='color: green;'>True</span>   | <span style='color: green;'>2026-01-06 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  |

_the following columns where excluded from the result: `record_hash, dp_load_timestamp, change_type`_

