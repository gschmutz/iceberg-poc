# Testing Update Operation with correction in the past

This test validates an UPDATE operation of one entity (with a new version) on a set of existing entities.
## Test Step 1
Insert 3 entities into raw table and perform initial SCD2 merge.


**Raw Table `raw_person`**


|   id | first_name   | last_name   | city   | email                    | status   | dp_valid_from       | dp_loaded_at        |
|------|--------------|-------------|--------|--------------------------|----------|---------------------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |



**Input to Merge**


| merge_key   | dp_key   |   id | first_name   | last_name   | city   | email                    | src_dp_valid_from   | load_ts             | status   | change_classification   | operation_type   | tgt_dp_valid_from   | tgt_dp_valid_to     | prev_dp_valid_from   | prev_dp_valid_to   |
|-------------|----------|------|--------------|-------------|--------|--------------------------|---------------------|---------------------|----------|-------------------------|------------------|---------------------|---------------------|----------------------|--------------------|
|             |          |    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 | ACTIVE   | NEW                     | UPDATE_EXISTING  | 9999-12-31 23:59:59 | 9999-12-31 23:59:59 |                      |                    |
|             |          |    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 | ACTIVE   | NEW                     | UPDATE_EXISTING  | 9999-12-31 23:59:59 | 9999-12-31 23:59:59 |                      |                    |
|             |          |    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 | ACTIVE   | NEW                     | UPDATE_EXISTING  | 9999-12-31 23:59:59 | 9999-12-31 23:59:59 |                      |                    |



**Dimensional Table `dim_person`**


| dp_key                                                                  | id                                   | first_name                               | last_name                                 | city                                      | email                                                       | dp_valid_from                                          | dp_valid_to                                            | dp_is_active                            | dp_is_latest                            | dp_created_at                                          | dp_replaced_at                                         |
|-------------------------------------------------------------------------|--------------------------------------|------------------------------------------|-------------------------------------------|-------------------------------------------|-------------------------------------------------------------|--------------------------------------------------------|--------------------------------------------------------|-----------------------------------------|-----------------------------------------|--------------------------------------------------------|--------------------------------------------------------|
| <span style='color: green;'>e152d470-f21a-4919-b2dc-989a19815faf</span> | <span style='color: green;'>1</span> | <span style='color: green;'>Alice</span> | <span style='color: green;'>Meyer</span>  | <span style='color: green;'>Zurich</span> | <span style='color: green;'>alice.meyer@example.com</span>  | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>3f3ec57b-fb99-41f4-bbcd-f48c426ece1c</span> | <span style='color: green;'>2</span> | <span style='color: green;'>Bob</span>   | <span style='color: green;'>Keller</span> | <span style='color: green;'>Bern</span>   | <span style='color: green;'>bob.keller@example.com</span>   | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>4639006c-f17f-4ac2-baf9-7064a858780a</span> | <span style='color: green;'>3</span> | <span style='color: green;'>Clara</span> | <span style='color: green;'>Schmid</span> | <span style='color: green;'>Basel</span>  | <span style='color: green;'>clara.schmid@example.com</span> | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |

_the following columns where excluded from the result: `record_hash, dp_load_timestamp, change_type`_

## Test Step 2
At 2026-01-10 00:00:00, update entity with `id=1` by setting city to bern and perform SCD2 merge.


**Raw Table `raw_person`**


|   id | first_name   | last_name   | city   | email                    | status   | dp_valid_from       | dp_loaded_at        |
|------|--------------|-------------|--------|--------------------------|----------|---------------------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    1 | Alice        | Meyer       | Bern   | alice.meyer@example.com  | ACTIVE   | 2026-01-10 00:00:00 | 2026-01-10 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-10 00:00:00 | 2026-01-10 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-10 00:00:00 | 2026-01-10 00:00:00 |



**Input to Merge**


| merge_key                            | dp_key                               |   id | first_name   | last_name   | city   | email                   | src_dp_valid_from   | load_ts             | status   | change_classification   | operation_type     | tgt_dp_valid_from   | tgt_dp_valid_to     | prev_dp_valid_from   | prev_dp_valid_to   |
|--------------------------------------|--------------------------------------|------|--------------|-------------|--------|-------------------------|---------------------|---------------------|----------|-------------------------|--------------------|---------------------|---------------------|----------------------|--------------------|
| e152d470-f21a-4919-b2dc-989a19815faf | e152d470-f21a-4919-b2dc-989a19815faf |    1 | Alice        | Meyer       | Bern   | alice.meyer@example.com | 2026-01-10 00:00:00 | 2026-01-10 00:00:00 | ACTIVE   | CHANGED                 | UPDATE_EXISTING    | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 |                      |                    |
|                                      | e152d470-f21a-4919-b2dc-989a19815faf |    1 | Alice        | Meyer       | Bern   | alice.meyer@example.com | 2026-01-10 00:00:00 | 2026-01-10 00:00:00 | ACTIVE   | CHANGED                 | INSERT_NEW_VERSION | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 |                      |                    |



**Dimensional Table `dim_person`**


| dp_key                                                                  | id                                   | first_name                               | last_name                                | city                                    | email                                                      | dp_valid_from                                          | dp_valid_to                                             | dp_is_active                              | dp_is_latest                              | dp_created_at                                          | dp_replaced_at                                          |
|-------------------------------------------------------------------------|--------------------------------------|------------------------------------------|------------------------------------------|-----------------------------------------|------------------------------------------------------------|--------------------------------------------------------|---------------------------------------------------------|-------------------------------------------|-------------------------------------------|--------------------------------------------------------|---------------------------------------------------------|
| e152d470-f21a-4919-b2dc-989a19815faf                                    | 1                                    | Alice                                    | Meyer                                    | Zurich                                  | alice.meyer@example.com                                    | 2026-01-01 00:00:00                                    | <span style='color: orange;'>2026-01-09 23:59:59</span> | <span style='color: orange;'>False</span> | <span style='color: orange;'>False</span> | 2026-01-02 00:00:00                                    | <span style='color: orange;'>2026-01-11 00:00:00</span> |
| <span style='color: green;'>157ac80e-caae-4cc1-90dd-705663440fb8</span> | <span style='color: green;'>1</span> | <span style='color: green;'>Alice</span> | <span style='color: green;'>Meyer</span> | <span style='color: green;'>Bern</span> | <span style='color: green;'>alice.meyer@example.com</span> | <span style='color: green;'>2026-01-10 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  | <span style='color: green;'>True</span>   | <span style='color: green;'>True</span>   | <span style='color: green;'>2026-01-11 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  |
| 3f3ec57b-fb99-41f4-bbcd-f48c426ece1c                                    | 2                                    | Bob                                      | Keller                                   | Bern                                    | bob.keller@example.com                                     | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                     | True                                      | True                                      | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                     |
| 4639006c-f17f-4ac2-baf9-7064a858780a                                    | 3                                    | Clara                                    | Schmid                                   | Basel                                   | clara.schmid@example.com                                   | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                     | True                                      | True                                      | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                     |

_the following columns where excluded from the result: `record_hash, dp_load_timestamp, change_type`_

## Test Step 3
At 2026-01-05 00:00:00, update entity with `id=1` in raw table by setting city to basel and perform SCD2 merge.


**Raw Table `raw_person`**


|   id | first_name   | last_name   | city   | email                    | status   | dp_valid_from       | dp_loaded_at        |
|------|--------------|-------------|--------|--------------------------|----------|---------------------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    1 | Alice        | Meyer       | Basel  | alice.meyer@example.com  | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    1 | Alice        | Meyer       | Bern   | alice.meyer@example.com  | ACTIVE   | 2026-01-10 00:00:00 | 2026-01-10 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-10 00:00:00 | 2026-01-10 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-10 00:00:00 | 2026-01-10 00:00:00 |



**Input to Merge**


| merge_key                            | dp_key                               |   id | first_name   | last_name   | city   | email                   | src_dp_valid_from   | load_ts             | status   | change_classification   | operation_type     | tgt_dp_valid_from   | tgt_dp_valid_to     | prev_dp_valid_from   | prev_dp_valid_to   |
|--------------------------------------|--------------------------------------|------|--------------|-------------|--------|-------------------------|---------------------|---------------------|----------|-------------------------|--------------------|---------------------|---------------------|----------------------|--------------------|
| e152d470-f21a-4919-b2dc-989a19815faf | e152d470-f21a-4919-b2dc-989a19815faf |    1 | Alice        | Meyer       | Basel  | alice.meyer@example.com | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 | ACTIVE   | CHANGED                 | UPDATE_EXISTING    | 2026-01-01 00:00:00 | 2026-01-09 23:59:59 |                      |                    |
|                                      | e152d470-f21a-4919-b2dc-989a19815faf |    1 | Alice        | Meyer       | Basel  | alice.meyer@example.com | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 | ACTIVE   | CHANGED                 | INSERT_NEW_VERSION | 2026-01-01 00:00:00 | 2026-01-09 23:59:59 |                      |                    |



**Dimensional Table `dim_person`**


| dp_key                                                                  | id                                   | first_name                               | last_name                                | city                                     | email                                                      | dp_valid_from                                          | dp_valid_to                                             | dp_is_active                             | dp_is_latest                             | dp_created_at                                          | dp_replaced_at                                          |
|-------------------------------------------------------------------------|--------------------------------------|------------------------------------------|------------------------------------------|------------------------------------------|------------------------------------------------------------|--------------------------------------------------------|---------------------------------------------------------|------------------------------------------|------------------------------------------|--------------------------------------------------------|---------------------------------------------------------|
| e152d470-f21a-4919-b2dc-989a19815faf                                    | 1                                    | Alice                                    | Meyer                                    | Zurich                                   | alice.meyer@example.com                                    | 2026-01-01 00:00:00                                    | <span style='color: orange;'>2026-01-04 23:59:59</span> | False                                    | False                                    | 2026-01-02 00:00:00                                    | <span style='color: orange;'>2026-01-16 00:00:00</span> |
| <span style='color: green;'>93ff414c-6ecb-449a-bc56-7dda17080a7f</span> | <span style='color: green;'>1</span> | <span style='color: green;'>Alice</span> | <span style='color: green;'>Meyer</span> | <span style='color: green;'>Basel</span> | <span style='color: green;'>alice.meyer@example.com</span> | <span style='color: green;'>2026-01-05 00:00:00</span> | <span style='color: green;'>2026-01-09 23:59:59</span>  | <span style='color: green;'>False</span> | <span style='color: green;'>False</span> | <span style='color: green;'>2026-01-16 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  |
| 157ac80e-caae-4cc1-90dd-705663440fb8                                    | 1                                    | Alice                                    | Meyer                                    | Bern                                     | alice.meyer@example.com                                    | 2026-01-10 00:00:00                                    | 9999-12-31 23:59:59                                     | True                                     | True                                     | 2026-01-11 00:00:00                                    | 9999-12-31 23:59:59                                     |
| 3f3ec57b-fb99-41f4-bbcd-f48c426ece1c                                    | 2                                    | Bob                                      | Keller                                   | Bern                                     | bob.keller@example.com                                     | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                     | True                                     | True                                     | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                     |
| 4639006c-f17f-4ac2-baf9-7064a858780a                                    | 3                                    | Clara                                    | Schmid                                   | Basel                                    | clara.schmid@example.com                                   | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                     | True                                     | True                                     | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                     |

_the following columns where excluded from the result: `record_hash, dp_load_timestamp, change_type`_

