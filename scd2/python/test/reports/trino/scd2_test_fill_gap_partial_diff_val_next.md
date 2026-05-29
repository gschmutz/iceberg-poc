# Testing partially filling a gap with a different value than the next version after the gap

This test validates filling the gap in a single entity. The record added into the gap is having a different value than the version following the gap.


 * **Strategy:** `trino`
 * **Last Run:** `2026-05-29 20:21:26`
At 2026-01-01 00:00:00, insert 3 records, at 2026-01-05 00:00:00 delete the one with id=3 and reinsert id=3 at 2026-01-15 00:00:00 into raw table and perform initial SCD2 merge.
### Perform Preparation


**Raw Table `raw_person`**


|   id | first_name   | last_name   | city   | email                    | status   | dp_ts_from          | dp_loaded_at        |
|------|--------------|-------------|--------|--------------------------|----------|---------------------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | INACTIVE | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-15 00:00:00 | 2026-01-15 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-15 00:00:00 | 2026-01-15 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-15 00:00:00 | 2026-01-15 00:00:00 |



**Dimensional Table `dim_person`**


| dp_record_id                         |   id | first_name   | last_name   | city   | email                    | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   | dp_load_ts          | dp_replace_ts       |
|--------------------------------------|------|--------------|-------------|--------|--------------------------|---------------------|---------------------|----------------|----------------|---------------------|---------------------|
| 1b8201bd-12d4-48ff-97fc-a04ed8c33008 |    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           | 2026-01-02 00:00:00 | 9999-12-31 23:59:59 |
| 1a66bdbf-0693-4b27-b756-be0bda8caa7e |    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           | 2026-01-02 00:00:00 | 9999-12-31 23:59:59 |
| e392eb35-6e00-4a2f-82ae-2af9d9b8822b |    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | 2026-01-01 00:00:00 | 2026-01-04 23:59:59 | False          | False          | 2026-01-02 00:00:00 | 2026-01-16 00:00:00 |
| f6bf2cc1-d36c-4795-b52e-07f6b9ad51de |    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | 2026-01-15 00:00:00 | 9999-12-31 23:59:59 | True           | True           | 2026-01-16 00:00:00 | 9999-12-31 23:59:59 |

_the following columns where excluded from the result: `dp_record_hash`_

## Test Step 2
Fill the gap partially at 2026-01-10 00:00:00 by adding a record with the same values as the version following the gap.


**Raw Table `raw_person`**


|   id | first_name   | last_name   | city   | email                    | status   | dp_ts_from          | dp_loaded_at        |
|------|--------------|-------------|--------|--------------------------|----------|---------------------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | INACTIVE | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-15 00:00:00 | 2026-01-15 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-15 00:00:00 | 2026-01-15 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-15 00:00:00 | 2026-01-15 00:00:00 |
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-10 00:00:00 | 2026-01-20 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-10 00:00:00 | 2026-01-20 00:00:00 |
|    3 | Clara        | Schmid      | Geneva | clara.schmid@example.com | ACTIVE   | 2026-01-10 00:00:00 | 2026-01-20 00:00:00 |



**Input to Merge**


| merge_record_id   | dp_record_id   |   id | first_name   | last_name   | city   | email                    | dp_record_hash                                                   | dp_del_flag   | operation_type     | case_name   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   |
|-------------------|----------------|------|--------------|-------------|--------|--------------------------|------------------------------------------------------------------|---------------|--------------------|-------------|---------------------|---------------------|----------------|----------------|
|                   |                |    3 | Clara        | Schmid      | Geneva | clara.schmid@example.com | 777BB26D490500D4BF4E829691C85C2DF112D21B4D205D879812E5BE99529853 | ACTIVE        | INSERT_NEW_VERSION | CASE_19     | 2026-01-10 00:00:00 | 2026-01-14 23:59:59 | False          | False          |



**Dimensional Table `dim_person`**


| dp_record_id                                                            | id                                   | first_name                               | last_name                                 | city                                      | email                                                       | dp_ts_from                                             | dp_ts_to                                               | dp_is_active                             | dp_is_latest                             | dp_load_ts                                             | dp_replace_ts                                          |
|-------------------------------------------------------------------------|--------------------------------------|------------------------------------------|-------------------------------------------|-------------------------------------------|-------------------------------------------------------------|--------------------------------------------------------|--------------------------------------------------------|------------------------------------------|------------------------------------------|--------------------------------------------------------|--------------------------------------------------------|
| 1b8201bd-12d4-48ff-97fc-a04ed8c33008                                    | 1                                    | Alice                                    | Meyer                                     | Zurich                                    | alice.meyer@example.com                                     | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                    | True                                     | True                                     | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                    |
| 1a66bdbf-0693-4b27-b756-be0bda8caa7e                                    | 2                                    | Bob                                      | Keller                                    | Bern                                      | bob.keller@example.com                                      | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                    | True                                     | True                                     | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                    |
| e392eb35-6e00-4a2f-82ae-2af9d9b8822b                                    | 3                                    | Clara                                    | Schmid                                    | Basel                                     | clara.schmid@example.com                                    | 2026-01-01 00:00:00                                    | 2026-01-04 23:59:59                                    | False                                    | False                                    | 2026-01-02 00:00:00                                    | 2026-01-16 00:00:00                                    |
| <span style='color: green;'>23d6c532-7cf1-4267-ba6f-f72a1269fe5a</span> | <span style='color: green;'>3</span> | <span style='color: green;'>Clara</span> | <span style='color: green;'>Schmid</span> | <span style='color: green;'>Geneva</span> | <span style='color: green;'>clara.schmid@example.com</span> | <span style='color: green;'>2026-01-10 00:00:00</span> | <span style='color: green;'>2026-01-14 23:59:59</span> | <span style='color: green;'>False</span> | <span style='color: green;'>False</span> | <span style='color: green;'>2026-01-21 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| f6bf2cc1-d36c-4795-b52e-07f6b9ad51de                                    | 3                                    | Clara                                    | Schmid                                    | Basel                                     | clara.schmid@example.com                                    | 2026-01-15 00:00:00                                    | 9999-12-31 23:59:59                                    | True                                     | True                                     | 2026-01-16 00:00:00                                    | 9999-12-31 23:59:59                                    |

_the following columns where excluded from the result: `dp_record_hash`_

