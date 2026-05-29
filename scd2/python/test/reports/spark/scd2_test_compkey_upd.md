# Testing Update Operation with Composite Key

This test validates an UPDATE operation of one entity (with a new version) on a set of existing entities.


 * **Strategy:** `spark`
 * **Last Run:** `2026-05-07 13:41:54`
## Test Step 1
At 2026-01-01 00:00:00, insert 3 entities into raw table and perform initial SCD2 merge.


**Raw Table `raw_person`**


|   id1 |   id2 | first_name   | last_name   | city   | email                    | status   | dp_ts_from          | dp_loaded_at        |
|-------|-------|--------------|-------------|--------|--------------------------|----------|---------------------|---------------------|
|     1 |     1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|     2 |     2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|     3 |     3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |



**Input to Merge**


| merge_record_id   | dp_record_id                         |   id1 |   id2 | first_name   | last_name   | city   | email                    | dp_record_hash                                                   | dp_del_flag   | operation_type     | case_name   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   |
|-------------------|--------------------------------------|-------|-------|--------------|-------------|--------|--------------------------|------------------------------------------------------------------|---------------|--------------------|-------------|---------------------|---------------------|----------------|----------------|
|                   | 3f220e58-86aa-4069-8e2e-a8871be20630 |     1 |     1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | BAB631B19480D6A34C9E6E4116135C91C1992DECA676BE09461E204DB7117991 | ACTIVE        | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           |
|                   | df11b76a-d508-4966-aad4-b8826337ca18 |     2 |     2 | Bob          | Keller      | Bern   | bob.keller@example.com   | 6B43397E6417EB6BFA808C396FC60FBCC77917F193CE46B75BA002FFA6B307B8 | ACTIVE        | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           |
|                   | 9557111e-366b-4808-9cfe-36b67d8506ce |     3 |     3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | F0C895E2DF779EE0ADFE3FBD536F11B632A0945F2424B469736A4F9861E47136 | ACTIVE        | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           |



**Dimensional Table `dim_person`**


| dp_record_id                                                            | id1                                  | id2                                  | first_name                               | last_name                                 | city                                      | email                                                       | dp_ts_from                                             | dp_ts_to                                               | dp_is_active                            | dp_is_latest                            | dp_load_ts                                             | dp_replace_ts                                          |
|-------------------------------------------------------------------------|--------------------------------------|--------------------------------------|------------------------------------------|-------------------------------------------|-------------------------------------------|-------------------------------------------------------------|--------------------------------------------------------|--------------------------------------------------------|-----------------------------------------|-----------------------------------------|--------------------------------------------------------|--------------------------------------------------------|
| <span style='color: green;'>cf59a4c7-c7f2-4834-93a9-fb3ad07e9338</span> | <span style='color: green;'>1</span> | <span style='color: green;'>1</span> | <span style='color: green;'>Alice</span> | <span style='color: green;'>Meyer</span>  | <span style='color: green;'>Zurich</span> | <span style='color: green;'>alice.meyer@example.com</span>  | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>0fe398ec-0b01-478c-ad18-ddac54f5b08e</span> | <span style='color: green;'>2</span> | <span style='color: green;'>2</span> | <span style='color: green;'>Bob</span>   | <span style='color: green;'>Keller</span> | <span style='color: green;'>Bern</span>   | <span style='color: green;'>bob.keller@example.com</span>   | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>56298583-b7cb-4858-af73-63be4d2b5702</span> | <span style='color: green;'>3</span> | <span style='color: green;'>3</span> | <span style='color: green;'>Clara</span> | <span style='color: green;'>Schmid</span> | <span style='color: green;'>Basel</span>  | <span style='color: green;'>clara.schmid@example.com</span> | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |

_the following columns where excluded from the result: `dp_record_hash`_

## Test Step 2
At 2026-01-05 00:00:00, update `email` of entity with `id=3` in raw table and perform SCD2 merge.


**Raw Table `raw_person`**


|   id1 |   id2 | first_name   | last_name   | city   | email                    | status   | dp_ts_from          | dp_loaded_at        |
|-------|-------|--------------|-------------|--------|--------------------------|----------|---------------------|---------------------|
|     1 |     1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|     2 |     2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|     3 |     3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|     1 |     1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|     2 |     2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|     3 |     3 | Clara        | Schmid      | Basel  | clara.schmid@newmail.com | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |



**Input to Merge**


| merge_record_id                      | dp_record_id                         |   id1 |   id2 | first_name   | last_name   | city   | email                    | dp_record_hash                                                   | dp_del_flag   | operation_type     | case_name   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   |
|--------------------------------------|--------------------------------------|-------|-------|--------------|-------------|--------|--------------------------|------------------------------------------------------------------|---------------|--------------------|-------------|---------------------|---------------------|----------------|----------------|
| 56298583-b7cb-4858-af73-63be4d2b5702 | 56298583-b7cb-4858-af73-63be4d2b5702 |     3 |     3 | Clara        | Schmid      | Basel  | clara.schmid@newmail.com | 951F006FFFFA92F38C798C862716CF6863F0ACC00693D08A27661D68AF2BA987 | ACTIVE        | UPDATE_VERSION     | CASE_11     | 2026-01-01 00:00:00 | 2026-01-04 23:59:59 | False          | False          |
|                                      | 6a42b282-1db2-4838-9838-c4999cbf4b31 |     3 |     3 | Clara        | Schmid      | Basel  | clara.schmid@newmail.com | 951F006FFFFA92F38C798C862716CF6863F0ACC00693D08A27661D68AF2BA987 | ACTIVE        | INSERT_NEW_VERSION | CASE_11     | 2026-01-05 00:00:00 | 9999-12-31 23:59:59 | True           | True           |



**Dimensional Table `dim_person`**


| dp_record_id                                                            | id1                                  | id2                                  | first_name                               | last_name                                 | city                                     | email                                                       | dp_ts_from                                             | dp_ts_to                                                | dp_is_active                              | dp_is_latest                              | dp_load_ts                                             | dp_replace_ts                                           |
|-------------------------------------------------------------------------|--------------------------------------|--------------------------------------|------------------------------------------|-------------------------------------------|------------------------------------------|-------------------------------------------------------------|--------------------------------------------------------|---------------------------------------------------------|-------------------------------------------|-------------------------------------------|--------------------------------------------------------|---------------------------------------------------------|
| cf59a4c7-c7f2-4834-93a9-fb3ad07e9338                                    | 1                                    | 1                                    | Alice                                    | Meyer                                     | Zurich                                   | alice.meyer@example.com                                     | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                     | True                                      | True                                      | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                     |
| 0fe398ec-0b01-478c-ad18-ddac54f5b08e                                    | 2                                    | 2                                    | Bob                                      | Keller                                    | Bern                                     | bob.keller@example.com                                      | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                     | True                                      | True                                      | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                     |
| 56298583-b7cb-4858-af73-63be4d2b5702                                    | 3                                    | 3                                    | Clara                                    | Schmid                                    | Basel                                    | clara.schmid@example.com                                    | 2026-01-01 00:00:00                                    | <span style='color: orange;'>2026-01-04 23:59:59</span> | <span style='color: orange;'>False</span> | <span style='color: orange;'>False</span> | 2026-01-02 00:00:00                                    | <span style='color: orange;'>2026-01-06 00:00:00</span> |
| <span style='color: green;'>90a25f9c-9c11-414a-8629-7840046c95e4</span> | <span style='color: green;'>3</span> | <span style='color: green;'>3</span> | <span style='color: green;'>Clara</span> | <span style='color: green;'>Schmid</span> | <span style='color: green;'>Basel</span> | <span style='color: green;'>clara.schmid@newmail.com</span> | <span style='color: green;'>2026-01-05 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  | <span style='color: green;'>True</span>   | <span style='color: green;'>True</span>   | <span style='color: green;'>2026-01-06 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  |

_the following columns where excluded from the result: `dp_record_hash`_

