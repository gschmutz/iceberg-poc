# Testing Delta Streaming Operation

This test validates an INSERT operation of one new entity (with a 1st version) and an UPDATE of an existing entity.


 * **Strategy:** `pyspark`
 * **Last Run:** `2026-05-07 14:17:13`
## Test Step 1
Insert 3 entities with different `dp_ts_from`timestamps into raw table and perform initial SCD2 merge.


**Raw Table `raw_person`**


|   id | first_name   | last_name   | city   | email                    | status   | dp_ts_from          | dp_loaded_at        |
|------|--------------|-------------|--------|--------------------------|----------|---------------------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-01 09:00:00 | 2026-01-01 09:20:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-01 09:10:00 | 2026-01-01 09:20:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-01 09:18:00 | 2026-01-01 09:20:00 |



**Input to Merge**


| merge_record_id   | dp_record_id                         |   id | first_name   | last_name   | city   | email                    | dp_record_hash                                                   | dp_del_flag   | operation_type     | case_name   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   |
|-------------------|--------------------------------------|------|--------------|-------------|--------|--------------------------|------------------------------------------------------------------|---------------|--------------------|-------------|---------------------|---------------------|----------------|----------------|
|                   | 66ccbcd6-6035-4135-a57f-09756e57eeaa |    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | F244BC679F09400F6966D6472E66C079A59943FEC38344C4306149D8034ED570 | ACTIVE        | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 09:00:00 | 9999-12-31 23:59:59 | True           | True           |
|                   | 7289cee8-c18d-4e53-8b54-46f8425d1a49 |    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | E5181C5926D5185F99D4654A7F15E2476045F4808F22C5924E128B87DEB6F93F | ACTIVE        | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 09:10:00 | 9999-12-31 23:59:59 | True           | True           |
|                   | f6a6b0d5-2d6a-409c-8076-4e58fa9203f2 |    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | 254677EA92F6E7E5A9C2629DE097CA5B2821DC6CF93B283D7158EC37920083CB | ACTIVE        | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 09:18:00 | 9999-12-31 23:59:59 | True           | True           |



**Dimensional Table `dim_person`**


| dp_record_id                                                            | id                                   | first_name                               | last_name                                 | city                                      | email                                                       | dp_ts_from                                             | dp_ts_to                                               | dp_is_active                            | dp_is_latest                            | dp_load_ts                                             | dp_replace_ts                                          |
|-------------------------------------------------------------------------|--------------------------------------|------------------------------------------|-------------------------------------------|-------------------------------------------|-------------------------------------------------------------|--------------------------------------------------------|--------------------------------------------------------|-----------------------------------------|-----------------------------------------|--------------------------------------------------------|--------------------------------------------------------|
| <span style='color: green;'>66ccbcd6-6035-4135-a57f-09756e57eeaa</span> | <span style='color: green;'>1</span> | <span style='color: green;'>Alice</span> | <span style='color: green;'>Meyer</span>  | <span style='color: green;'>Zurich</span> | <span style='color: green;'>alice.meyer@example.com</span>  | <span style='color: green;'>2026-01-01 09:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-01 09:25:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>7289cee8-c18d-4e53-8b54-46f8425d1a49</span> | <span style='color: green;'>2</span> | <span style='color: green;'>Bob</span>   | <span style='color: green;'>Keller</span> | <span style='color: green;'>Bern</span>   | <span style='color: green;'>bob.keller@example.com</span>   | <span style='color: green;'>2026-01-01 09:10:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-01 09:25:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>f6a6b0d5-2d6a-409c-8076-4e58fa9203f2</span> | <span style='color: green;'>3</span> | <span style='color: green;'>Clara</span> | <span style='color: green;'>Schmid</span> | <span style='color: green;'>Basel</span>  | <span style='color: green;'>clara.schmid@example.com</span> | <span style='color: green;'>2026-01-01 09:18:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-01 09:25:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |

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
| 66ccbcd6-6035-4135-a57f-09756e57eeaa | 66ccbcd6-6035-4135-a57f-09756e57eeaa |    1 | Alice        | Meyer       | Zurich | alice.meyer@newmail.com  | 58322BFA8CD1A815212088E6536AB1571A9A3319343645810F5E0AA4D9614CD2 | ACTIVE        | UPDATE_VERSION     | CASE_11     | 2026-01-01 09:00:00 | 2026-01-01 09:24:59 | False          | False          |
|                                      | 0e7a99ba-122b-40f5-92a4-ae1d029c17c0 |    1 | Alice        | Meyer       | Zurich | alice.meyer@newmail.com  | 58322BFA8CD1A815212088E6536AB1571A9A3319343645810F5E0AA4D9614CD2 | ACTIVE        | INSERT_NEW_VERSION | CASE_11     | 2026-01-01 09:25:00 | 9999-12-31 23:59:59 | True           | True           |
|                                      | 0e0f3129-ab3e-4355-84cd-0b29d7017b61 |   10 | Kevin        | Loosli      | Bern   | kevin.loosli@example.com | 147D854645B1DCD25836FE6D0474D717C87972550B48027F01305BE396767F20 | ACTIVE        | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 09:25:00 | 9999-12-31 23:59:59 | True           | True           |



**Dimensional Table `dim_person`**


| dp_record_id                                                            | id                                    | first_name                               | last_name                                 | city                                      | email                                                       | dp_ts_from                                             | dp_ts_to                                                | dp_is_active                              | dp_is_latest                              | dp_load_ts                                             | dp_replace_ts                                           |
|-------------------------------------------------------------------------|---------------------------------------|------------------------------------------|-------------------------------------------|-------------------------------------------|-------------------------------------------------------------|--------------------------------------------------------|---------------------------------------------------------|-------------------------------------------|-------------------------------------------|--------------------------------------------------------|---------------------------------------------------------|
| 66ccbcd6-6035-4135-a57f-09756e57eeaa                                    | 1                                     | Alice                                    | Meyer                                     | Zurich                                    | alice.meyer@example.com                                     | 2026-01-01 09:00:00                                    | <span style='color: orange;'>2026-01-01 09:24:59</span> | <span style='color: orange;'>False</span> | <span style='color: orange;'>False</span> | 2026-01-01 09:25:00                                    | <span style='color: orange;'>2026-01-01 09:35:00</span> |
| <span style='color: green;'>0e7a99ba-122b-40f5-92a4-ae1d029c17c0</span> | <span style='color: green;'>1</span>  | <span style='color: green;'>Alice</span> | <span style='color: green;'>Meyer</span>  | <span style='color: green;'>Zurich</span> | <span style='color: green;'>alice.meyer@newmail.com</span>  | <span style='color: green;'>2026-01-01 09:25:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  | <span style='color: green;'>True</span>   | <span style='color: green;'>True</span>   | <span style='color: green;'>2026-01-01 09:35:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  |
| 7289cee8-c18d-4e53-8b54-46f8425d1a49                                    | 2                                     | Bob                                      | Keller                                    | Bern                                      | bob.keller@example.com                                      | 2026-01-01 09:10:00                                    | 9999-12-31 23:59:59                                     | True                                      | True                                      | 2026-01-01 09:25:00                                    | 9999-12-31 23:59:59                                     |
| f6a6b0d5-2d6a-409c-8076-4e58fa9203f2                                    | 3                                     | Clara                                    | Schmid                                    | Basel                                     | clara.schmid@example.com                                    | 2026-01-01 09:18:00                                    | 9999-12-31 23:59:59                                     | True                                      | True                                      | 2026-01-01 09:25:00                                    | 9999-12-31 23:59:59                                     |
| <span style='color: green;'>0e0f3129-ab3e-4355-84cd-0b29d7017b61</span> | <span style='color: green;'>10</span> | <span style='color: green;'>Kevin</span> | <span style='color: green;'>Loosli</span> | <span style='color: green;'>Bern</span>   | <span style='color: green;'>kevin.loosli@example.com</span> | <span style='color: green;'>2026-01-01 09:25:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  | <span style='color: green;'>True</span>   | <span style='color: green;'>True</span>   | <span style='color: green;'>2026-01-01 09:35:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  |

_the following columns where excluded from the result: `dp_record_hash`_

