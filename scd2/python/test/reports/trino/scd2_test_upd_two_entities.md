# Testing Update Operation of 2 entities

This test validates an UPDATE operation of two entities (with new versions) on a set of existing entities.
## Test Step 1
At 2026-01-01 00:00:00, insert 3 entities into raw table and perform initial SCD2 merge.


**Raw Table `raw_person`**


|   id | first_name   | last_name   | city   | email                    | status   | dp_ts_from          | dp_loaded_at        |
|------|--------------|-------------|--------|--------------------------|----------|---------------------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |



**Input to Merge**


| merge_key   | dp_key   |   id | first_name   | last_name   | city   | email                    | record_hash                                                      | load_ts             | status   | operation_type     | case_name   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   |
|-------------|----------|------|--------------|-------------|--------|--------------------------|------------------------------------------------------------------|---------------------|----------|--------------------|-------------|---------------------|---------------------|----------------|----------------|
|             |          |    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | 00B9A7122065F01BE7FD23C6FB962AEE6DE3B84D0BA50409DC26FC5A150FBDC8 | 2026-01-01 00:00:00 | ACTIVE   | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           |
|             |          |    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | D28A23C8422275E006FCF3D86AA51CF4E058FB495B8E48560FC9BF7BCC019B40 | 2026-01-01 00:00:00 | ACTIVE   | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           |
|             |          |    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | 77C069EE2AA3730894A6E3319ADC455C203B6CC4D35B0B912C2FAADF3C687676 | 2026-01-01 00:00:00 | ACTIVE   | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           |



**Dimensional Table `dim_person`**


| dp_key                                                                  | id                                   | first_name                               | last_name                                 | city                                      | email                                                       | dp_ts_from                                             | dp_ts_to                                               | dp_is_active                            | dp_is_latest                            | dp_created_at                                          | dp_replaced_at                                         |
|-------------------------------------------------------------------------|--------------------------------------|------------------------------------------|-------------------------------------------|-------------------------------------------|-------------------------------------------------------------|--------------------------------------------------------|--------------------------------------------------------|-----------------------------------------|-----------------------------------------|--------------------------------------------------------|--------------------------------------------------------|
| <span style='color: green;'>2c40d01d-f660-4efb-8584-4eb1b7bb85ce</span> | <span style='color: green;'>1</span> | <span style='color: green;'>Alice</span> | <span style='color: green;'>Meyer</span>  | <span style='color: green;'>Zurich</span> | <span style='color: green;'>alice.meyer@example.com</span>  | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>debc5ee5-1a1b-4ac4-8d47-24fc08f3e081</span> | <span style='color: green;'>2</span> | <span style='color: green;'>Bob</span>   | <span style='color: green;'>Keller</span> | <span style='color: green;'>Bern</span>   | <span style='color: green;'>bob.keller@example.com</span>   | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>4d4d7479-271c-4de4-9dea-5d6d219f7d56</span> | <span style='color: green;'>3</span> | <span style='color: green;'>Clara</span> | <span style='color: green;'>Schmid</span> | <span style='color: green;'>Basel</span>  | <span style='color: green;'>clara.schmid@example.com</span> | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |

_the following columns where excluded from the result: `record_hash, dp_load_timestamp, change_type`_

## Test Step 2
At 2026-01-05 00:00:00, update `city` of entity with `id=1` and `email` of entity with `id=3` in raw table and perform SCD2 merge.


**Raw Table `raw_person`**


|   id | first_name   | last_name   | city   | email                    | status   | dp_ts_from          | dp_loaded_at        |
|------|--------------|-------------|--------|--------------------------|----------|---------------------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    1 | Alice        | Meyer       | Bern   | alice.meyer@example.com  | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@newmail.com | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |



**Input to Merge**


| merge_key                            | dp_key                               |   id | first_name   | last_name   | city   | email                    | record_hash                                                      | load_ts             | status   | operation_type     | case_name   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   |
|--------------------------------------|--------------------------------------|------|--------------|-------------|--------|--------------------------|------------------------------------------------------------------|---------------------|----------|--------------------|-------------|---------------------|---------------------|----------------|----------------|
| 2c40d01d-f660-4efb-8584-4eb1b7bb85ce | 2c40d01d-f660-4efb-8584-4eb1b7bb85ce |    1 | Alice        | Meyer       | Bern   | alice.meyer@example.com  | 6449C8A21EC1B7B2BD4891618CF5853B27A97968D41570EE3CD34617BDBBD7BD | 2026-01-05 00:00:00 | ACTIVE   | UPDATE_VERSION     | CASE_11     | 2026-01-01 00:00:00 | 2026-01-04 23:59:59 | False          | False          |
| 4d4d7479-271c-4de4-9dea-5d6d219f7d56 | 4d4d7479-271c-4de4-9dea-5d6d219f7d56 |    3 | Clara        | Schmid      | Basel  | clara.schmid@newmail.com | 9477D9000CEDC6AA3E01D45847CE658798640D2C2E3614371B6FA40923F369C6 | 2026-01-05 00:00:00 | ACTIVE   | UPDATE_VERSION     | CASE_11     | 2026-01-01 00:00:00 | 2026-01-04 23:59:59 | False          | False          |
|                                      |                                      |    1 | Alice        | Meyer       | Bern   | alice.meyer@example.com  | 6449C8A21EC1B7B2BD4891618CF5853B27A97968D41570EE3CD34617BDBBD7BD | 2026-01-05 00:00:00 | ACTIVE   | INSERT_NEW_VERSION | CASE_11     | 2026-01-05 00:00:00 | 9999-12-31 23:59:59 | True           | True           |
|                                      |                                      |    3 | Clara        | Schmid      | Basel  | clara.schmid@newmail.com | 9477D9000CEDC6AA3E01D45847CE658798640D2C2E3614371B6FA40923F369C6 | 2026-01-05 00:00:00 | ACTIVE   | INSERT_NEW_VERSION | CASE_11     | 2026-01-05 00:00:00 | 9999-12-31 23:59:59 | True           | True           |



**Dimensional Table `dim_person`**


| dp_key                                                                  | id                                   | first_name                               | last_name                                 | city                                     | email                                                       | dp_ts_from                                             | dp_ts_to                                                | dp_is_active                              | dp_is_latest                              | dp_created_at                                          | dp_replaced_at                                          |
|-------------------------------------------------------------------------|--------------------------------------|------------------------------------------|-------------------------------------------|------------------------------------------|-------------------------------------------------------------|--------------------------------------------------------|---------------------------------------------------------|-------------------------------------------|-------------------------------------------|--------------------------------------------------------|---------------------------------------------------------|
| 2c40d01d-f660-4efb-8584-4eb1b7bb85ce                                    | 1                                    | Alice                                    | Meyer                                     | Zurich                                   | alice.meyer@example.com                                     | 2026-01-01 00:00:00                                    | <span style='color: orange;'>2026-01-04 23:59:59</span> | <span style='color: orange;'>False</span> | <span style='color: orange;'>False</span> | 2026-01-02 00:00:00                                    | <span style='color: orange;'>2026-01-06 00:00:00</span> |
| <span style='color: green;'>9dfd3b18-c775-4388-909f-baf7b5f1cacb</span> | <span style='color: green;'>1</span> | <span style='color: green;'>Alice</span> | <span style='color: green;'>Meyer</span>  | <span style='color: green;'>Bern</span>  | <span style='color: green;'>alice.meyer@example.com</span>  | <span style='color: green;'>2026-01-05 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  | <span style='color: green;'>True</span>   | <span style='color: green;'>True</span>   | <span style='color: green;'>2026-01-06 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  |
| debc5ee5-1a1b-4ac4-8d47-24fc08f3e081                                    | 2                                    | Bob                                      | Keller                                    | Bern                                     | bob.keller@example.com                                      | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                     | True                                      | True                                      | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                     |
| 4d4d7479-271c-4de4-9dea-5d6d219f7d56                                    | 3                                    | Clara                                    | Schmid                                    | Basel                                    | clara.schmid@example.com                                    | 2026-01-01 00:00:00                                    | <span style='color: orange;'>2026-01-04 23:59:59</span> | <span style='color: orange;'>False</span> | <span style='color: orange;'>False</span> | 2026-01-02 00:00:00                                    | <span style='color: orange;'>2026-01-06 00:00:00</span> |
| <span style='color: green;'>500212b4-c28c-419c-855f-26a0d688294b</span> | <span style='color: green;'>3</span> | <span style='color: green;'>Clara</span> | <span style='color: green;'>Schmid</span> | <span style='color: green;'>Basel</span> | <span style='color: green;'>clara.schmid@newmail.com</span> | <span style='color: green;'>2026-01-05 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  | <span style='color: green;'>True</span>   | <span style='color: green;'>True</span>   | <span style='color: green;'>2026-01-06 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  |

_the following columns where excluded from the result: `record_hash, dp_load_timestamp, change_type`_

