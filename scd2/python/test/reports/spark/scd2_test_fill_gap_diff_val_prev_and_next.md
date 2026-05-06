# Testing Filling a gap with different value than the next version after the gap

This test validates filling the gap in a single entity. The record added into the gap is having different values than the version following the gap.


 * **Strategy:** `spark`
 * **Last Run:** `2026-05-06 18:39:53`
At 2026-01-01 00:00:00, insert 3 records, at 2026-01-05 00:00:00 delete the one with id=3 and reinsert id=3 at 2026-01-10 00:00:00 into raw table and perform initial SCD2 merge.
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
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-10 00:00:00 | 2026-01-10 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-10 00:00:00 | 2026-01-10 00:00:00 |
|    3 | Clara        | Schmid      | Geneva | clara.schmid@example.com | ACTIVE   | 2026-01-10 00:00:00 | 2026-01-10 00:00:00 |



**Dimensional Table `dim_person`**


| dp_record_id                         |   id | first_name   | last_name   | city   | email                    | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   | dp_load_ts          | dp_replace_ts       |
|--------------------------------------|------|--------------|-------------|--------|--------------------------|---------------------|---------------------|----------------|----------------|---------------------|---------------------|
| cc512f59-3aba-4993-89e6-56e032b0c988 |    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           | 2026-01-02 00:00:00 | 9999-12-31 23:59:59 |
| 1975cb97-765b-49fe-a2a4-634fe98b7f82 |    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           | 2026-01-02 00:00:00 | 9999-12-31 23:59:59 |
| 3f9f70cc-e5c7-4295-b2e4-9dbbc06f1578 |    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | 2026-01-01 00:00:00 | 2026-01-04 23:59:59 | False          | False          | 2026-01-02 00:00:00 | 2026-01-11 00:00:00 |
| 4d16f160-7560-45e8-81a5-ff7c0fc26821 |    3 | Clara        | Schmid      | Geneva | clara.schmid@example.com | 2026-01-10 00:00:00 | 9999-12-31 23:59:59 | True           | True           | 2026-01-11 00:00:00 | 9999-12-31 23:59:59 |

_the following columns where excluded from the result: `dp_record_hash`_

## Test Step 2
Fill the gap at 2026-01-05 00:00:00 by adding a record with different values as the version preceding and following the gap.


**Raw Table `raw_person`**


|   id | first_name   | last_name   | city   | email                    | status   | dp_ts_from          | dp_loaded_at        |
|------|--------------|-------------|--------|--------------------------|----------|---------------------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | INACTIVE | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-10 00:00:00 | 2026-01-10 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-10 00:00:00 | 2026-01-10 00:00:00 |
|    3 | Clara        | Schmid      | Geneva | clara.schmid@example.com | ACTIVE   | 2026-01-10 00:00:00 | 2026-01-10 00:00:00 |
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-15 00:00:00 | 2026-01-15 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-15 00:00:00 | 2026-01-15 00:00:00 |
|    3 | Clara        | Schmid      | Zurich | clara.schmid@example.com | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-15 00:00:00 |



**Input to Merge**


| merge_record_id   | dp_record_id                         |   id | first_name   | last_name   | city   | email                    | dp_record_hash                                                   | dp_del_flag   | operation_type     | case_name   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   |
|-------------------|--------------------------------------|------|--------------|-------------|--------|--------------------------|------------------------------------------------------------------|---------------|--------------------|-------------|---------------------|---------------------|----------------|----------------|
|                   | f484fb50-fae7-4162-8915-4f9a59261136 |    3 | Clara        | Schmid      | Zurich | clara.schmid@example.com | 24D761116FACE285ECC3E970FF105F256954CF40802EB4B2944296D89115D95C | ACTIVE        | INSERT_NEW_VERSION | CASE_27     | 2026-01-05 00:00:00 | 2026-01-09 23:59:59 | False          | False          |



**Dimensional Table `dim_person`**


| dp_record_id                                                            | id                                   | first_name                               | last_name                                 | city                                      | email                                                       | dp_ts_from                                             | dp_ts_to                                               | dp_is_active                             | dp_is_latest                             | dp_load_ts                                             | dp_replace_ts                                          |
|-------------------------------------------------------------------------|--------------------------------------|------------------------------------------|-------------------------------------------|-------------------------------------------|-------------------------------------------------------------|--------------------------------------------------------|--------------------------------------------------------|------------------------------------------|------------------------------------------|--------------------------------------------------------|--------------------------------------------------------|
| cc512f59-3aba-4993-89e6-56e032b0c988                                    | 1                                    | Alice                                    | Meyer                                     | Zurich                                    | alice.meyer@example.com                                     | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                    | True                                     | True                                     | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                    |
| 1975cb97-765b-49fe-a2a4-634fe98b7f82                                    | 2                                    | Bob                                      | Keller                                    | Bern                                      | bob.keller@example.com                                      | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                    | True                                     | True                                     | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                    |
| 3f9f70cc-e5c7-4295-b2e4-9dbbc06f1578                                    | 3                                    | Clara                                    | Schmid                                    | Basel                                     | clara.schmid@example.com                                    | 2026-01-01 00:00:00                                    | 2026-01-04 23:59:59                                    | False                                    | False                                    | 2026-01-02 00:00:00                                    | 2026-01-11 00:00:00                                    |
| <span style='color: green;'>39496c0a-0195-4bd4-b4b1-385a379a1d1d</span> | <span style='color: green;'>3</span> | <span style='color: green;'>Clara</span> | <span style='color: green;'>Schmid</span> | <span style='color: green;'>Zurich</span> | <span style='color: green;'>clara.schmid@example.com</span> | <span style='color: green;'>2026-01-05 00:00:00</span> | <span style='color: green;'>2026-01-09 23:59:59</span> | <span style='color: green;'>False</span> | <span style='color: green;'>False</span> | <span style='color: green;'>2026-01-16 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| 4d16f160-7560-45e8-81a5-ff7c0fc26821                                    | 3                                    | Clara                                    | Schmid                                    | Geneva                                    | clara.schmid@example.com                                    | 2026-01-10 00:00:00                                    | 9999-12-31 23:59:59                                    | True                                     | True                                     | 2026-01-11 00:00:00                                    | 9999-12-31 23:59:59                                    |

_the following columns where excluded from the result: `dp_record_hash`_

