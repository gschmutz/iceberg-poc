# Testing Update Operation of 2 entities

This test validates an UPDATE operation of two entities (with new versions) on a set of existing entities.


 * **Strategy:** `pyspark`
 * **Last Run:** `2026-05-07 14:27:34`
## Test Step 1
At 2026-01-01 00:00:00, insert 3 entities into raw table and perform initial SCD2 merge.


**Raw Table `raw_person`**


|   id | first_name   | last_name   | city   | email                    | status   | dp_ts_from          | dp_loaded_at        |
|------|--------------|-------------|--------|--------------------------|----------|---------------------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |



**Input to Merge**


| merge_record_id   | dp_record_id                         |   id | first_name   | last_name   | city   | email                    | dp_record_hash                                                   | dp_del_flag   | operation_type     | case_name   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   |
|-------------------|--------------------------------------|------|--------------|-------------|--------|--------------------------|------------------------------------------------------------------|---------------|--------------------|-------------|---------------------|---------------------|----------------|----------------|
|                   | d7c2a0a1-6c76-45f3-9250-6f1fe32a0dfe |    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | 00B9A7122065F01BE7FD23C6FB962AEE6DE3B84D0BA50409DC26FC5A150FBDC8 | ACTIVE        | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           |
|                   | 38c4307e-e097-4aa8-867f-0c2b83ce23d1 |    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | D28A23C8422275E006FCF3D86AA51CF4E058FB495B8E48560FC9BF7BCC019B40 | ACTIVE        | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           |
|                   | 90d6a050-b1dd-4244-8667-56d659d02bbe |    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | 77C069EE2AA3730894A6E3319ADC455C203B6CC4D35B0B912C2FAADF3C687676 | ACTIVE        | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           |



**Dimensional Table `dim_person`**


| dp_record_id                                                            | id                                   | first_name                               | last_name                                 | city                                      | email                                                       | dp_ts_from                                             | dp_ts_to                                               | dp_is_active                            | dp_is_latest                            | dp_load_ts                                             | dp_replace_ts                                          |
|-------------------------------------------------------------------------|--------------------------------------|------------------------------------------|-------------------------------------------|-------------------------------------------|-------------------------------------------------------------|--------------------------------------------------------|--------------------------------------------------------|-----------------------------------------|-----------------------------------------|--------------------------------------------------------|--------------------------------------------------------|
| <span style='color: green;'>d7c2a0a1-6c76-45f3-9250-6f1fe32a0dfe</span> | <span style='color: green;'>1</span> | <span style='color: green;'>Alice</span> | <span style='color: green;'>Meyer</span>  | <span style='color: green;'>Zurich</span> | <span style='color: green;'>alice.meyer@example.com</span>  | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>38c4307e-e097-4aa8-867f-0c2b83ce23d1</span> | <span style='color: green;'>2</span> | <span style='color: green;'>Bob</span>   | <span style='color: green;'>Keller</span> | <span style='color: green;'>Bern</span>   | <span style='color: green;'>bob.keller@example.com</span>   | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>90d6a050-b1dd-4244-8667-56d659d02bbe</span> | <span style='color: green;'>3</span> | <span style='color: green;'>Clara</span> | <span style='color: green;'>Schmid</span> | <span style='color: green;'>Basel</span>  | <span style='color: green;'>clara.schmid@example.com</span> | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |

_the following columns where excluded from the result: `dp_record_hash`_

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


| merge_record_id                      | dp_record_id                         |   id | first_name   | last_name   | city   | email                    | dp_record_hash                                                   | dp_del_flag   | operation_type     | case_name   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   |
|--------------------------------------|--------------------------------------|------|--------------|-------------|--------|--------------------------|------------------------------------------------------------------|---------------|--------------------|-------------|---------------------|---------------------|----------------|----------------|
| 90d6a050-b1dd-4244-8667-56d659d02bbe | 90d6a050-b1dd-4244-8667-56d659d02bbe |    3 | Clara        | Schmid      | Basel  | clara.schmid@newmail.com | 9477D9000CEDC6AA3E01D45847CE658798640D2C2E3614371B6FA40923F369C6 | ACTIVE        | UPDATE_VERSION     | CASE_11     | 2026-01-01 00:00:00 | 2026-01-04 23:59:59 | False          | False          |
| d7c2a0a1-6c76-45f3-9250-6f1fe32a0dfe | d7c2a0a1-6c76-45f3-9250-6f1fe32a0dfe |    1 | Alice        | Meyer       | Bern   | alice.meyer@example.com  | 6449C8A21EC1B7B2BD4891618CF5853B27A97968D41570EE3CD34617BDBBD7BD | ACTIVE        | UPDATE_VERSION     | CASE_11     | 2026-01-01 00:00:00 | 2026-01-04 23:59:59 | False          | False          |
|                                      | 7741cc06-ea4a-42de-ab97-7cb3ca12b475 |    1 | Alice        | Meyer       | Bern   | alice.meyer@example.com  | 6449C8A21EC1B7B2BD4891618CF5853B27A97968D41570EE3CD34617BDBBD7BD | ACTIVE        | INSERT_NEW_VERSION | CASE_11     | 2026-01-05 00:00:00 | 9999-12-31 23:59:59 | True           | True           |
|                                      | a4a7e16b-2a07-4296-9410-1551056e43e9 |    3 | Clara        | Schmid      | Basel  | clara.schmid@newmail.com | 9477D9000CEDC6AA3E01D45847CE658798640D2C2E3614371B6FA40923F369C6 | ACTIVE        | INSERT_NEW_VERSION | CASE_11     | 2026-01-05 00:00:00 | 9999-12-31 23:59:59 | True           | True           |



**Dimensional Table `dim_person`**


| dp_record_id                                                            | id                                   | first_name                               | last_name                                 | city                                     | email                                                       | dp_ts_from                                             | dp_ts_to                                                | dp_is_active                              | dp_is_latest                              | dp_load_ts                                             | dp_replace_ts                                           |
|-------------------------------------------------------------------------|--------------------------------------|------------------------------------------|-------------------------------------------|------------------------------------------|-------------------------------------------------------------|--------------------------------------------------------|---------------------------------------------------------|-------------------------------------------|-------------------------------------------|--------------------------------------------------------|---------------------------------------------------------|
| d7c2a0a1-6c76-45f3-9250-6f1fe32a0dfe                                    | 1                                    | Alice                                    | Meyer                                     | Zurich                                   | alice.meyer@example.com                                     | 2026-01-01 00:00:00                                    | <span style='color: orange;'>2026-01-04 23:59:59</span> | <span style='color: orange;'>False</span> | <span style='color: orange;'>False</span> | 2026-01-02 00:00:00                                    | <span style='color: orange;'>2026-01-06 00:00:00</span> |
| <span style='color: green;'>7741cc06-ea4a-42de-ab97-7cb3ca12b475</span> | <span style='color: green;'>1</span> | <span style='color: green;'>Alice</span> | <span style='color: green;'>Meyer</span>  | <span style='color: green;'>Bern</span>  | <span style='color: green;'>alice.meyer@example.com</span>  | <span style='color: green;'>2026-01-05 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  | <span style='color: green;'>True</span>   | <span style='color: green;'>True</span>   | <span style='color: green;'>2026-01-06 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  |
| 38c4307e-e097-4aa8-867f-0c2b83ce23d1                                    | 2                                    | Bob                                      | Keller                                    | Bern                                     | bob.keller@example.com                                      | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                     | True                                      | True                                      | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                     |
| 90d6a050-b1dd-4244-8667-56d659d02bbe                                    | 3                                    | Clara                                    | Schmid                                    | Basel                                    | clara.schmid@example.com                                    | 2026-01-01 00:00:00                                    | <span style='color: orange;'>2026-01-04 23:59:59</span> | <span style='color: orange;'>False</span> | <span style='color: orange;'>False</span> | 2026-01-02 00:00:00                                    | <span style='color: orange;'>2026-01-06 00:00:00</span> |
| <span style='color: green;'>a4a7e16b-2a07-4296-9410-1551056e43e9</span> | <span style='color: green;'>3</span> | <span style='color: green;'>Clara</span> | <span style='color: green;'>Schmid</span> | <span style='color: green;'>Basel</span> | <span style='color: green;'>clara.schmid@newmail.com</span> | <span style='color: green;'>2026-01-05 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  | <span style='color: green;'>True</span>   | <span style='color: green;'>True</span>   | <span style='color: green;'>2026-01-06 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  |

_the following columns where excluded from the result: `dp_record_hash`_

