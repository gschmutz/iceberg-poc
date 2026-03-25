# Testing Delta Streaming Operation

This test validates an INSERT operation of one new entity (with a 1st version) and an UPDATE of an existing entity.
 * **Strategy:** `pyspark`
 * **Last Run:** `2026-03-25 14:28:06`
## Test Step 1
Insert 3 entities with different `dp_ts_from`timestamps into raw table and perform initial SCD2 merge.


**Raw Table `raw_person`**


|   id | first_name   | last_name   | city   | email                    | status   | dp_ts_from          | dp_loaded_at        |
|------|--------------|-------------|--------|--------------------------|----------|---------------------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-01 09:00:00 | 2026-01-01 09:20:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-01 09:10:00 | 2026-01-01 09:20:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-01 09:18:00 | 2026-01-01 09:20:00 |



**Input to Merge**


| merge_key   | dp_key                               |   id | first_name   | last_name   | city   | email                    | record_hash                                                      | load_ts             | status   | operation_type     | case_name   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   |
|-------------|--------------------------------------|------|--------------|-------------|--------|--------------------------|------------------------------------------------------------------|---------------------|----------|--------------------|-------------|---------------------|---------------------|----------------|----------------|
|             | 1d5df182-5b33-417f-b81a-b727743fe81f |    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | 00B9A7122065F01BE7FD23C6FB962AEE6DE3B84D0BA50409DC26FC5A150FBDC8 | 2026-01-01 09:20:00 | ACTIVE   | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 09:00:00 | 9999-12-31 23:59:59 | True           | True           |
|             | 9e83cfb5-3e61-45e5-8515-eef59e42a347 |    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | D28A23C8422275E006FCF3D86AA51CF4E058FB495B8E48560FC9BF7BCC019B40 | 2026-01-01 09:20:00 | ACTIVE   | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 09:10:00 | 9999-12-31 23:59:59 | True           | True           |
|             | 9195acbc-4267-4238-a7e3-2d2e47ad941d |    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | 77C069EE2AA3730894A6E3319ADC455C203B6CC4D35B0B912C2FAADF3C687676 | 2026-01-01 09:20:00 | ACTIVE   | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 09:18:00 | 9999-12-31 23:59:59 | True           | True           |



**Dimensional Table `dim_person`**


| dp_key                                                                  | id                                   | first_name                               | last_name                                 | city                                      | email                                                       | dp_ts_from                                             | dp_ts_to                                               | dp_is_active                            | dp_is_latest                            | dp_created_at                                          | dp_replaced_at                                         |
|-------------------------------------------------------------------------|--------------------------------------|------------------------------------------|-------------------------------------------|-------------------------------------------|-------------------------------------------------------------|--------------------------------------------------------|--------------------------------------------------------|-----------------------------------------|-----------------------------------------|--------------------------------------------------------|--------------------------------------------------------|
| <span style='color: green;'>1d5df182-5b33-417f-b81a-b727743fe81f</span> | <span style='color: green;'>1</span> | <span style='color: green;'>Alice</span> | <span style='color: green;'>Meyer</span>  | <span style='color: green;'>Zurich</span> | <span style='color: green;'>alice.meyer@example.com</span>  | <span style='color: green;'>2026-01-01 09:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-01 09:25:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>9e83cfb5-3e61-45e5-8515-eef59e42a347</span> | <span style='color: green;'>2</span> | <span style='color: green;'>Bob</span>   | <span style='color: green;'>Keller</span> | <span style='color: green;'>Bern</span>   | <span style='color: green;'>bob.keller@example.com</span>   | <span style='color: green;'>2026-01-01 09:10:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-01 09:25:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>9195acbc-4267-4238-a7e3-2d2e47ad941d</span> | <span style='color: green;'>3</span> | <span style='color: green;'>Clara</span> | <span style='color: green;'>Schmid</span> | <span style='color: green;'>Basel</span>  | <span style='color: green;'>clara.schmid@example.com</span> | <span style='color: green;'>2026-01-01 09:18:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-01 09:25:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |

_the following columns where excluded from the result: `record_hash, dp_load_timestamp, change_type`_

## Test Step 2
At 2026-01-01 09:30:00, insert the new entity with `id=10` and update `email` of entity with `id=1` and perform SCD2 merge.


**Raw Table `raw_person`**


|   id | first_name   | last_name   | city   | email                    | status   | dp_ts_from          | dp_loaded_at        |
|------|--------------|-------------|--------|--------------------------|----------|---------------------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-01 09:00:00 | 2026-01-01 09:20:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-01 09:10:00 | 2026-01-01 09:20:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-01 09:18:00 | 2026-01-01 09:20:00 |
|    1 | Alice        | Meyer       | Zurich | alice.meyer@newmail.com  | ACTIVE   | 2026-01-01 09:25:00 | 2026-01-01 09:30:00 |
|   10 | Kevin        | Loosli      | Bern   | kevin.loosli@example.com | ACTIVE   | 2026-01-01 09:25:00 | 2026-01-01 09:30:00 |



**Input to Merge**


| merge_key                            | dp_key                               |   id | first_name   | last_name   | city   | email                    | record_hash                                                      | load_ts             | status   | operation_type     | case_name   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   |
|--------------------------------------|--------------------------------------|------|--------------|-------------|--------|--------------------------|------------------------------------------------------------------|---------------------|----------|--------------------|-------------|---------------------|---------------------|----------------|----------------|
| 1d5df182-5b33-417f-b81a-b727743fe81f | 1d5df182-5b33-417f-b81a-b727743fe81f |    1 | Alice        | Meyer       | Zurich | alice.meyer@newmail.com  | 1D79C5F2E5B741DA72ABA282938388262704B53CE6C5A174D3C76CCBDE55015F | 2026-01-01 09:30:00 | ACTIVE   | UPDATE_VERSION     | CASE_11     | 2026-01-01 09:00:00 | 2026-01-01 09:24:59 | False          | False          |
|                                      | 4cfc1ad1-d0f7-40f9-acd9-d9dad1b5d728 |    1 | Alice        | Meyer       | Zurich | alice.meyer@newmail.com  | 1D79C5F2E5B741DA72ABA282938388262704B53CE6C5A174D3C76CCBDE55015F | 2026-01-01 09:30:00 | ACTIVE   | INSERT_NEW_VERSION | CASE_11     | 2026-01-01 09:25:00 | 9999-12-31 23:59:59 | True           | True           |
|                                      | b718e862-36c1-418f-b9df-ce91fc91b0bf |   10 | Kevin        | Loosli      | Bern   | kevin.loosli@example.com | F32E425B7483AA533A0DBD8DB41BBD3DEEDBD2FF6427D420A7130EC9B174787C | 2026-01-01 09:30:00 | ACTIVE   | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 09:25:00 | 9999-12-31 23:59:59 | True           | True           |



**Dimensional Table `dim_person`**


| dp_key                                                                  | id                                    | first_name                               | last_name                                 | city                                      | email                                                       | dp_ts_from                                             | dp_ts_to                                                | dp_is_active                              | dp_is_latest                              | dp_created_at                                          | dp_replaced_at                                          |
|-------------------------------------------------------------------------|---------------------------------------|------------------------------------------|-------------------------------------------|-------------------------------------------|-------------------------------------------------------------|--------------------------------------------------------|---------------------------------------------------------|-------------------------------------------|-------------------------------------------|--------------------------------------------------------|---------------------------------------------------------|
| 1d5df182-5b33-417f-b81a-b727743fe81f                                    | 1                                     | Alice                                    | Meyer                                     | Zurich                                    | alice.meyer@example.com                                     | 2026-01-01 09:00:00                                    | <span style='color: orange;'>2026-01-01 09:24:59</span> | <span style='color: orange;'>False</span> | <span style='color: orange;'>False</span> | 2026-01-01 09:25:00                                    | <span style='color: orange;'>2026-01-01 09:35:00</span> |
| <span style='color: green;'>4cfc1ad1-d0f7-40f9-acd9-d9dad1b5d728</span> | <span style='color: green;'>1</span>  | <span style='color: green;'>Alice</span> | <span style='color: green;'>Meyer</span>  | <span style='color: green;'>Zurich</span> | <span style='color: green;'>alice.meyer@newmail.com</span>  | <span style='color: green;'>2026-01-01 09:25:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  | <span style='color: green;'>True</span>   | <span style='color: green;'>True</span>   | <span style='color: green;'>2026-01-01 09:35:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  |
| 9e83cfb5-3e61-45e5-8515-eef59e42a347                                    | 2                                     | Bob                                      | Keller                                    | Bern                                      | bob.keller@example.com                                      | 2026-01-01 09:10:00                                    | 9999-12-31 23:59:59                                     | True                                      | True                                      | 2026-01-01 09:25:00                                    | 9999-12-31 23:59:59                                     |
| 9195acbc-4267-4238-a7e3-2d2e47ad941d                                    | 3                                     | Clara                                    | Schmid                                    | Basel                                     | clara.schmid@example.com                                    | 2026-01-01 09:18:00                                    | 9999-12-31 23:59:59                                     | True                                      | True                                      | 2026-01-01 09:25:00                                    | 9999-12-31 23:59:59                                     |
| <span style='color: green;'>b718e862-36c1-418f-b9df-ce91fc91b0bf</span> | <span style='color: green;'>10</span> | <span style='color: green;'>Kevin</span> | <span style='color: green;'>Loosli</span> | <span style='color: green;'>Bern</span>   | <span style='color: green;'>kevin.loosli@example.com</span> | <span style='color: green;'>2026-01-01 09:25:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  | <span style='color: green;'>True</span>   | <span style='color: green;'>True</span>   | <span style='color: green;'>2026-01-01 09:35:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  |

_the following columns where excluded from the result: `record_hash, dp_load_timestamp, change_type`_

