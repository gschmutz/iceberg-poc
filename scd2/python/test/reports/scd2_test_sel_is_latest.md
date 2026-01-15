# Testing for valid data at a given at a given timestamp

This test validates multiple UPDATE operations on one entity over time producing many versions.
## Test Step 2
Insert 2 entities into raw table, perform initial SCD2 merge and then do an update.
### Raw Table `raw_person`

|   id | first_name   | last_name   | city   | email                   | status   | dp_exported_at      |
|------|--------------|-------------|--------|-------------------------|----------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com | ACTIVE   | 2026-01-01 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com  | ACTIVE   | 2026-01-01 00:00:00 |
|    1 | Alice        | Meyer       | Bern   | alice.meyer@example.com | ACTIVE   | 2026-01-05 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com  | ACTIVE   | 2026-01-05 00:00:00 |

### Input to Merge

|   merge_key |   id | first_name   | last_name   | city   | email                   | load_ts             | status   | change_classification   | operation_type     |
|-------------|------|--------------|-------------|--------|-------------------------|---------------------|----------|-------------------------|--------------------|
|           1 |    1 | Alice        | Meyer       | Bern   | alice.meyer@example.com | 2026-01-05 00:00:00 | ACTIVE   | CHANGED                 | UPDATE_EXISTING    |
|         nan |    1 | Alice        | Meyer       | Bern   | alice.meyer@example.com | 2026-01-05 00:00:00 | ACTIVE   | CHANGED                 | INSERT_NEW_VERSION |

### Dimensional Table `dim_person`

| id                                   | first_name                               | last_name                                | city                                    | email                                                      | dp_valid_from                                          | dp_valid_to                                             | dp_is_active                              | dp_is_latest                              | dp_created_at                                          | dp_replaced_at                                          |
|--------------------------------------|------------------------------------------|------------------------------------------|-----------------------------------------|------------------------------------------------------------|--------------------------------------------------------|---------------------------------------------------------|-------------------------------------------|-------------------------------------------|--------------------------------------------------------|---------------------------------------------------------|
| 1                                    | Alice                                    | Meyer                                    | Zurich                                  | alice.meyer@example.com                                    | 2026-01-01 00:00:00                                    | <span style='color: orange;'>2026-01-04 23:59:59</span> | <span style='color: orange;'>False</span> | <span style='color: orange;'>False</span> | 2026-01-02 00:00:00                                    | <span style='color: orange;'>2026-01-06 00:00:00</span> |
| <span style='color: green;'>1</span> | <span style='color: green;'>Alice</span> | <span style='color: green;'>Meyer</span> | <span style='color: green;'>Bern</span> | <span style='color: green;'>alice.meyer@example.com</span> | <span style='color: green;'>2026-01-05 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  | <span style='color: green;'>True</span>   | <span style='color: green;'>True</span>   | <span style='color: green;'>2026-01-06 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  |
| 2                                    | Bob                                      | Keller                                   | Bern                                    | bob.keller@example.com                                     | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                     | True                                      | True                                      | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                     |

_the following columns where excluded from the result: `record_hash, dp_load_timestamp, change_type`_

### Dim Table `dim_person` Result

|   id | first_name   | last_name   | city   | email                   | dp_valid_from       | dp_valid_to         | dp_is_active   | dp_is_latest   | dp_load_timestamp   | dp_created_at       | dp_replaced_at      | change_type   | record_hash                                                      |
|------|--------------|-------------|--------|-------------------------|---------------------|---------------------|----------------|----------------|---------------------|---------------------|---------------------|---------------|------------------------------------------------------------------|
|    1 | Alice        | Meyer       | Bern   | alice.meyer@example.com | 2026-01-05 00:00:00 | 9999-12-31 23:59:59 | True           | True           | 2026-01-06 00:00:00 | 2026-01-06 00:00:00 | 9999-12-31 23:59:59 | SUPERSEDED_BY | 67B1EB7F635FBBC16C2FFA0EAD786E929C4D1F8E26B210ABFE37D0CFB73EDE39 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com  | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           | 2026-01-02 00:00:00 | 2026-01-02 00:00:00 | 9999-12-31 23:59:59 | NEW           | 68844625A41E2D2540D4A17FBC7B51B3733C95FC58817DA05765F111F4F659CE |

