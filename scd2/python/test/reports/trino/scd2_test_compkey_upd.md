# Testing Update Operation with Composite Key

This test validates an UPDATE operation of one entity (with a new version) on a set of existing entities.
## Test Step 1
At 2026-01-01 00:00:00, insert 3 entities into raw table and perform initial SCD2 merge.


**Raw Table `raw_person`**


|   id1 |   id2 | first_name   | last_name   | city   | email                    | status   | dp_ts_from          | dp_loaded_at        |
|-------|-------|--------------|-------------|--------|--------------------------|----------|---------------------|---------------------|
|     1 |     1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|     2 |     2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|     3 |     3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |



**Input to Merge**


| merge_key   | dp_key   |   id1 |   id2 | first_name   | last_name   | city   | email                    | record_hash                                                      | load_ts             | status   | operation_type     | case_name   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   |
|-------------|----------|-------|-------|--------------|-------------|--------|--------------------------|------------------------------------------------------------------|---------------------|----------|--------------------|-------------|---------------------|---------------------|----------------|----------------|
|             |          |     1 |     1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | FC32620040E739795BE9C7EF23702C97E362C4C2BAAC8B6CAADE58A27DC1087A | 2026-01-01 00:00:00 | ACTIVE   | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           |
|             |          |     2 |     2 | Bob          | Keller      | Bern   | bob.keller@example.com   | BF95C839ED40F6745B2FFB0B3988C93FC14D92CD490A0BB26013F7A1F4748986 | 2026-01-01 00:00:00 | ACTIVE   | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           |
|             |          |     3 |     3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | EFA3962E9F15A846EB1999A38C6B310F71E88BEDC22CEE2174B9C2B8A121524E | 2026-01-01 00:00:00 | ACTIVE   | INSERT_NEW_VERSION | CASE_1      | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 | True           | True           |



**Dimensional Table `dim_person`**


| dp_key                                                                  | id1                                  | id2                                  | first_name                               | last_name                                 | city                                      | email                                                       | dp_ts_from                                             | dp_ts_to                                               | dp_is_active                            | dp_is_latest                            | dp_created_at                                          | dp_replaced_at                                         |
|-------------------------------------------------------------------------|--------------------------------------|--------------------------------------|------------------------------------------|-------------------------------------------|-------------------------------------------|-------------------------------------------------------------|--------------------------------------------------------|--------------------------------------------------------|-----------------------------------------|-----------------------------------------|--------------------------------------------------------|--------------------------------------------------------|
| <span style='color: green;'>f4d6f711-4052-4ec4-b900-4f1cd29348d0</span> | <span style='color: green;'>1</span> | <span style='color: green;'>1</span> | <span style='color: green;'>Alice</span> | <span style='color: green;'>Meyer</span>  | <span style='color: green;'>Zurich</span> | <span style='color: green;'>alice.meyer@example.com</span>  | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>3b5e1e96-9737-44f5-9b79-255116a0285f</span> | <span style='color: green;'>2</span> | <span style='color: green;'>2</span> | <span style='color: green;'>Bob</span>   | <span style='color: green;'>Keller</span> | <span style='color: green;'>Bern</span>   | <span style='color: green;'>bob.keller@example.com</span>   | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>cb3d6d4c-ce08-453a-89b5-0ecdbed264ec</span> | <span style='color: green;'>3</span> | <span style='color: green;'>3</span> | <span style='color: green;'>Clara</span> | <span style='color: green;'>Schmid</span> | <span style='color: green;'>Basel</span>  | <span style='color: green;'>clara.schmid@example.com</span> | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |

_the following columns where excluded from the result: `record_hash, dp_load_timestamp, change_type`_

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


| merge_key                            | dp_key                               |   id1 |   id2 | first_name   | last_name   | city   | email                    | record_hash                                                      | load_ts             | status   | operation_type     | case_name   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   |
|--------------------------------------|--------------------------------------|-------|-------|--------------|-------------|--------|--------------------------|------------------------------------------------------------------|---------------------|----------|--------------------|-------------|---------------------|---------------------|----------------|----------------|
| cb3d6d4c-ce08-453a-89b5-0ecdbed264ec | cb3d6d4c-ce08-453a-89b5-0ecdbed264ec |     3 |     3 | Clara        | Schmid      | Basel  | clara.schmid@newmail.com | 8C01D872535978047B58D1C33D3CE3731B4E1A08F5E6F10D9659FB72C94807B1 | 2026-01-05 00:00:00 | ACTIVE   | UPDATE_VERSION     | CASE_11     | 2026-01-01 00:00:00 | 2026-01-04 23:59:59 | False          | False          |
|                                      |                                      |     3 |     3 | Clara        | Schmid      | Basel  | clara.schmid@newmail.com | 8C01D872535978047B58D1C33D3CE3731B4E1A08F5E6F10D9659FB72C94807B1 | 2026-01-05 00:00:00 | ACTIVE   | INSERT_NEW_VERSION | CASE_11     | 2026-01-05 00:00:00 | 9999-12-31 23:59:59 | True           | True           |



**Dimensional Table `dim_person`**


| dp_key                                                                  | id1                                  | id2                                  | first_name                               | last_name                                 | city                                     | email                                                       | dp_ts_from                                             | dp_ts_to                                                | dp_is_active                              | dp_is_latest                              | dp_created_at                                          | dp_replaced_at                                          |
|-------------------------------------------------------------------------|--------------------------------------|--------------------------------------|------------------------------------------|-------------------------------------------|------------------------------------------|-------------------------------------------------------------|--------------------------------------------------------|---------------------------------------------------------|-------------------------------------------|-------------------------------------------|--------------------------------------------------------|---------------------------------------------------------|
| f4d6f711-4052-4ec4-b900-4f1cd29348d0                                    | 1                                    | 1                                    | Alice                                    | Meyer                                     | Zurich                                   | alice.meyer@example.com                                     | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                     | True                                      | True                                      | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                     |
| 3b5e1e96-9737-44f5-9b79-255116a0285f                                    | 2                                    | 2                                    | Bob                                      | Keller                                    | Bern                                     | bob.keller@example.com                                      | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                     | True                                      | True                                      | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                     |
| cb3d6d4c-ce08-453a-89b5-0ecdbed264ec                                    | 3                                    | 3                                    | Clara                                    | Schmid                                    | Basel                                    | clara.schmid@example.com                                    | 2026-01-01 00:00:00                                    | <span style='color: orange;'>2026-01-04 23:59:59</span> | <span style='color: orange;'>False</span> | <span style='color: orange;'>False</span> | 2026-01-02 00:00:00                                    | <span style='color: orange;'>2026-01-06 00:00:00</span> |
| <span style='color: green;'>c7d132a1-b04c-4ecb-9532-94060f78c8ee</span> | <span style='color: green;'>3</span> | <span style='color: green;'>3</span> | <span style='color: green;'>Clara</span> | <span style='color: green;'>Schmid</span> | <span style='color: green;'>Basel</span> | <span style='color: green;'>clara.schmid@newmail.com</span> | <span style='color: green;'>2026-01-05 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  | <span style='color: green;'>True</span>   | <span style='color: green;'>True</span>   | <span style='color: green;'>2026-01-06 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  |

_the following columns where excluded from the result: `record_hash, dp_load_timestamp, change_type`_

