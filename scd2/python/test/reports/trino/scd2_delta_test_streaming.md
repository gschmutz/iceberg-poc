# Testing Delta Streaming Operation

This test validates an INSERT operation of one new entity (with a 1st version) and an UPDATE of an existing entity.


 * **Strategy:** `trino`
 * **Last Run:** `2026-05-07 07:56:28`
## Test Step 1
Insert 3 entities with different `dp_ts_from`timestamps into raw table and perform initial SCD2 merge.


**Raw Table `raw_person`**


|   id | first_name   | last_name   | city   | email                    | status   | dp_ts_from          | dp_loaded_at        |
|------|--------------|-------------|--------|--------------------------|----------|---------------------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-01 09:00:00 | 2026-01-01 09:20:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-01 09:10:00 | 2026-01-01 09:20:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-01 09:18:00 | 2026-01-01 09:20:00 |



**Input to Merge**


| merge_record_id   | dp_record_id   |   id | first_name   | last_name   | city   | email                    | dp_record_hash                                                   | dp_del_flag   | operation_type     | case_name   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   |
|-------------------|----------------|------|--------------|-------------|--------|--------------------------|------------------------------------------------------------------|---------------|--------------------|-------------|---------------------|---------------------|----------------|----------------|
|                   |                |    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | 00B9A7122065F01BE7FD23C6FB962AEE6DE3B84D0BA50409DC26FC5A150FBDC8 | ACTIVE        | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 09:00:00 | 9999-12-31 23:59:59 | True           | True           |
|                   |                |    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | D28A23C8422275E006FCF3D86AA51CF4E058FB495B8E48560FC9BF7BCC019B40 | ACTIVE        | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 09:10:00 | 9999-12-31 23:59:59 | True           | True           |
|                   |                |    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | 77C069EE2AA3730894A6E3319ADC455C203B6CC4D35B0B912C2FAADF3C687676 | ACTIVE        | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 09:18:00 | 9999-12-31 23:59:59 | True           | True           |



**Dimensional Table `dim_person`**


| dp_record_id                                                            | id                                   | first_name                               | last_name                                 | city                                      | email                                                       | dp_ts_from                                             | dp_ts_to                                               | dp_is_active                            | dp_is_latest                            | dp_load_ts                                             | dp_replace_ts                                          |
|-------------------------------------------------------------------------|--------------------------------------|------------------------------------------|-------------------------------------------|-------------------------------------------|-------------------------------------------------------------|--------------------------------------------------------|--------------------------------------------------------|-----------------------------------------|-----------------------------------------|--------------------------------------------------------|--------------------------------------------------------|
| <span style='color: green;'>a6c9d136-3fb1-4acd-9784-cefc35e0ebf8</span> | <span style='color: green;'>1</span> | <span style='color: green;'>Alice</span> | <span style='color: green;'>Meyer</span>  | <span style='color: green;'>Zurich</span> | <span style='color: green;'>alice.meyer@example.com</span>  | <span style='color: green;'>2026-01-01 09:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-01 09:25:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>d7e6f4c4-fcaa-4647-9bcd-a6e1af5e2b67</span> | <span style='color: green;'>2</span> | <span style='color: green;'>Bob</span>   | <span style='color: green;'>Keller</span> | <span style='color: green;'>Bern</span>   | <span style='color: green;'>bob.keller@example.com</span>   | <span style='color: green;'>2026-01-01 09:10:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-01 09:25:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>a8897426-a4c0-47d6-8903-df454d63cd3a</span> | <span style='color: green;'>3</span> | <span style='color: green;'>Clara</span> | <span style='color: green;'>Schmid</span> | <span style='color: green;'>Basel</span>  | <span style='color: green;'>clara.schmid@example.com</span> | <span style='color: green;'>2026-01-01 09:18:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-01 09:25:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |

_the following columns where excluded from the result: `dp_record_hash`_

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


| merge_record_id                      | dp_record_id                         |   id | first_name   | last_name   | city   | email                    | dp_record_hash                                                   | dp_del_flag   | operation_type     | case_name   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   |
|--------------------------------------|--------------------------------------|------|--------------|-------------|--------|--------------------------|------------------------------------------------------------------|---------------|--------------------|-------------|---------------------|---------------------|----------------|----------------|
| a6c9d136-3fb1-4acd-9784-cefc35e0ebf8 | a6c9d136-3fb1-4acd-9784-cefc35e0ebf8 |    1 | Alice        | Meyer       | Zurich | alice.meyer@newmail.com  | 1D79C5F2E5B741DA72ABA282938388262704B53CE6C5A174D3C76CCBDE55015F | ACTIVE        | UPDATE_VERSION     | CASE_11     | 2026-01-01 09:00:00 | 2026-01-01 09:24:59 | False          | False          |
|                                      |                                      |    1 | Alice        | Meyer       | Zurich | alice.meyer@newmail.com  | 1D79C5F2E5B741DA72ABA282938388262704B53CE6C5A174D3C76CCBDE55015F | ACTIVE        | INSERT_NEW_VERSION | CASE_11     | 2026-01-01 09:25:00 | 9999-12-31 23:59:59 | True           | True           |
|                                      |                                      |   10 | Kevin        | Loosli      | Bern   | kevin.loosli@example.com | F32E425B7483AA533A0DBD8DB41BBD3DEEDBD2FF6427D420A7130EC9B174787C | ACTIVE        | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 09:25:00 | 9999-12-31 23:59:59 | True           | True           |



**Dimensional Table `dim_person`**


| dp_record_id                                                            | id                                    | first_name                               | last_name                                 | city                                      | email                                                       | dp_ts_from                                             | dp_ts_to                                                | dp_is_active                              | dp_is_latest                              | dp_load_ts                                             | dp_replace_ts                                           |
|-------------------------------------------------------------------------|---------------------------------------|------------------------------------------|-------------------------------------------|-------------------------------------------|-------------------------------------------------------------|--------------------------------------------------------|---------------------------------------------------------|-------------------------------------------|-------------------------------------------|--------------------------------------------------------|---------------------------------------------------------|
| a6c9d136-3fb1-4acd-9784-cefc35e0ebf8                                    | 1                                     | Alice                                    | Meyer                                     | Zurich                                    | alice.meyer@example.com                                     | 2026-01-01 09:00:00                                    | <span style='color: orange;'>2026-01-01 09:24:59</span> | <span style='color: orange;'>False</span> | <span style='color: orange;'>False</span> | 2026-01-01 09:25:00                                    | <span style='color: orange;'>2026-01-01 09:35:00</span> |
| <span style='color: green;'>2b7c1cc9-e0f6-4962-989c-f372853b9d21</span> | <span style='color: green;'>1</span>  | <span style='color: green;'>Alice</span> | <span style='color: green;'>Meyer</span>  | <span style='color: green;'>Zurich</span> | <span style='color: green;'>alice.meyer@newmail.com</span>  | <span style='color: green;'>2026-01-01 09:25:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  | <span style='color: green;'>True</span>   | <span style='color: green;'>True</span>   | <span style='color: green;'>2026-01-01 09:35:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  |
| d7e6f4c4-fcaa-4647-9bcd-a6e1af5e2b67                                    | 2                                     | Bob                                      | Keller                                    | Bern                                      | bob.keller@example.com                                      | 2026-01-01 09:10:00                                    | 9999-12-31 23:59:59                                     | True                                      | True                                      | 2026-01-01 09:25:00                                    | 9999-12-31 23:59:59                                     |
| a8897426-a4c0-47d6-8903-df454d63cd3a                                    | 3                                     | Clara                                    | Schmid                                    | Basel                                     | clara.schmid@example.com                                    | 2026-01-01 09:18:00                                    | 9999-12-31 23:59:59                                     | True                                      | True                                      | 2026-01-01 09:25:00                                    | 9999-12-31 23:59:59                                     |
| <span style='color: green;'>4804df56-2fbb-437f-bfb5-0e975a2e5f41</span> | <span style='color: green;'>10</span> | <span style='color: green;'>Kevin</span> | <span style='color: green;'>Loosli</span> | <span style='color: green;'>Bern</span>   | <span style='color: green;'>kevin.loosli@example.com</span> | <span style='color: green;'>2026-01-01 09:25:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  | <span style='color: green;'>True</span>   | <span style='color: green;'>True</span>   | <span style='color: green;'>2026-01-01 09:35:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  |

_the following columns where excluded from the result: `dp_record_hash`_

