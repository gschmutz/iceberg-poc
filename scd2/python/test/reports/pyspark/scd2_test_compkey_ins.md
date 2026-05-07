# Testing Insert Operation with Composite Key

This test validates an INSERT operation of one new entity (with a 1st version) into a set of existing entities.


 * **Strategy:** `pyspark`
 * **Last Run:** `2026-05-07 08:48:47`
## Test Step 1
Insert 3 entities into raw table and perform initial SCD2 merge.


**Raw Table `raw_person`**


|   id1 |   id2 | first_name   | last_name   | city   | email                    | status   | dp_ts_from          | dp_loaded_at        |
|-------|-------|--------------|-------------|--------|--------------------------|----------|---------------------|---------------------|
|     1 |     1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|     2 |     2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|     3 |     3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |



**Input to Merge**


| merge_record_id   | dp_record_id                         |   id1 |   id2 | first_name   | last_name   | city   | email                    | dp_record_hash                                                   | dp_del_flag   | operation_type     | case_name   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   |
|-------------------|--------------------------------------|-------|-------|--------------|-------------|--------|--------------------------|------------------------------------------------------------------|---------------|--------------------|-------------|---------------------|---------------------|----------------|----------------|
|                   | dd375aa1-e649-4460-be85-70b9f4d05f6b |     1 |     1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | FC32620040E739795BE9C7EF23702C97E362C4C2BAAC8B6CAADE58A27DC1087A | ACTIVE        | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           |
|                   | 7ef3de05-465e-4ed7-a9ed-3383acd1621e |     2 |     2 | Bob          | Keller      | Bern   | bob.keller@example.com   | BF95C839ED40F6745B2FFB0B3988C93FC14D92CD490A0BB26013F7A1F4748986 | ACTIVE        | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           |
|                   | b4bd07dc-0ea8-448c-a8b1-b1615717a1f2 |     3 |     3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | EFA3962E9F15A846EB1999A38C6B310F71E88BEDC22CEE2174B9C2B8A121524E | ACTIVE        | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           |



**Dimensional Table `dim_person`**


| dp_record_id                                                            | id1                                  | id2                                  | first_name                               | last_name                                 | city                                      | email                                                       | dp_ts_from                                             | dp_ts_to                                               | dp_is_active                            | dp_is_latest                            | dp_load_ts                                             | dp_replace_ts                                          |
|-------------------------------------------------------------------------|--------------------------------------|--------------------------------------|------------------------------------------|-------------------------------------------|-------------------------------------------|-------------------------------------------------------------|--------------------------------------------------------|--------------------------------------------------------|-----------------------------------------|-----------------------------------------|--------------------------------------------------------|--------------------------------------------------------|
| <span style='color: green;'>dd375aa1-e649-4460-be85-70b9f4d05f6b</span> | <span style='color: green;'>1</span> | <span style='color: green;'>1</span> | <span style='color: green;'>Alice</span> | <span style='color: green;'>Meyer</span>  | <span style='color: green;'>Zurich</span> | <span style='color: green;'>alice.meyer@example.com</span>  | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>7ef3de05-465e-4ed7-a9ed-3383acd1621e</span> | <span style='color: green;'>2</span> | <span style='color: green;'>2</span> | <span style='color: green;'>Bob</span>   | <span style='color: green;'>Keller</span> | <span style='color: green;'>Bern</span>   | <span style='color: green;'>bob.keller@example.com</span>   | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>b4bd07dc-0ea8-448c-a8b1-b1615717a1f2</span> | <span style='color: green;'>3</span> | <span style='color: green;'>3</span> | <span style='color: green;'>Clara</span> | <span style='color: green;'>Schmid</span> | <span style='color: green;'>Basel</span>  | <span style='color: green;'>clara.schmid@example.com</span> | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |

_the following columns where excluded from the result: `dp_record_hash`_

## Test Step 2
At 2026-01-05 00:00:00, insert the new entity with `id=10` into the new partition of the raw table and perform SCD2 merge.


**Raw Table `raw_person`**


|   id1 |   id2 | first_name   | last_name   | city   | email                    | status   | dp_ts_from          | dp_loaded_at        |
|-------|-------|--------------|-------------|--------|--------------------------|----------|---------------------|---------------------|
|     1 |     1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|     2 |     2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|     3 |     3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|     1 |     1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|     2 |     2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|     3 |     3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    10 |    10 | Kevin        | Loosli      | Bern   | kevin.loosli@example.com | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |



**Input to Merge**


| merge_record_id   | dp_record_id                         |   id1 |   id2 | first_name   | last_name   | city   | email                    | dp_record_hash                                                   | dp_del_flag   | operation_type     | case_name   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   |
|-------------------|--------------------------------------|-------|-------|--------------|-------------|--------|--------------------------|------------------------------------------------------------------|---------------|--------------------|-------------|---------------------|---------------------|----------------|----------------|
|                   | 7b51c858-694b-485c-a220-17c66833b29e |    10 |    10 | Kevin        | Loosli      | Bern   | kevin.loosli@example.com | DB6D2FC1F766B81756761381B965CE5D13E4AE3F8BF50E66BB2188214DC1B55C | ACTIVE        | INSERT_NEW_VERSION | CASE_1      | 2026-01-05 00:00:00 | 9999-12-31 23:59:59 | True           | True           |



**Dimensional Table `dim_person`**


| dp_record_id                                                            | id1                                   | id2                                   | first_name                               | last_name                                 | city                                    | email                                                       | dp_ts_from                                             | dp_ts_to                                               | dp_is_active                            | dp_is_latest                            | dp_load_ts                                             | dp_replace_ts                                          |
|-------------------------------------------------------------------------|---------------------------------------|---------------------------------------|------------------------------------------|-------------------------------------------|-----------------------------------------|-------------------------------------------------------------|--------------------------------------------------------|--------------------------------------------------------|-----------------------------------------|-----------------------------------------|--------------------------------------------------------|--------------------------------------------------------|
| dd375aa1-e649-4460-be85-70b9f4d05f6b                                    | 1                                     | 1                                     | Alice                                    | Meyer                                     | Zurich                                  | alice.meyer@example.com                                     | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                    | True                                    | True                                    | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                    |
| 7ef3de05-465e-4ed7-a9ed-3383acd1621e                                    | 2                                     | 2                                     | Bob                                      | Keller                                    | Bern                                    | bob.keller@example.com                                      | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                    | True                                    | True                                    | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                    |
| b4bd07dc-0ea8-448c-a8b1-b1615717a1f2                                    | 3                                     | 3                                     | Clara                                    | Schmid                                    | Basel                                   | clara.schmid@example.com                                    | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                    | True                                    | True                                    | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                    |
| <span style='color: green;'>7b51c858-694b-485c-a220-17c66833b29e</span> | <span style='color: green;'>10</span> | <span style='color: green;'>10</span> | <span style='color: green;'>Kevin</span> | <span style='color: green;'>Loosli</span> | <span style='color: green;'>Bern</span> | <span style='color: green;'>kevin.loosli@example.com</span> | <span style='color: green;'>2026-01-05 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-06 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |

_the following columns where excluded from the result: `dp_record_hash`_

