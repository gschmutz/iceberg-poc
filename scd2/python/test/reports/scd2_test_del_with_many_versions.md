# Testing Physical Delete Operation with many versions

This test validates a DELETE operation of a single entity with many versions. The delete is created by a physical delete in the raw table, i.e., the entity is removed from the raw table partition.
## Test Step 1
At 2026-01-01 00:00:00, insert 3 entities into raw table and perform initial SCD2 merge. At 2026-01-05 00:00:00, update email address of entity with `id=3` and perform SCD2 merge.


**Raw Table `raw_person`**


|   id | first_name   | last_name   | city   | email                    | status   | dp_valid_from       | dp_loaded_at        |
|------|--------------|-------------|--------|--------------------------|----------|---------------------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@newmail.com | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |



**Input to Merge**


| merge_key                            | dp_key                               |   id | first_name   | last_name   | city   | email                    | src_dp_valid_from   | load_ts             | status   | change_classification   | operation_type     | tgt_dp_valid_from   | tgt_dp_valid_to     | prev_dp_valid_from   | prev_dp_valid_to   |
|--------------------------------------|--------------------------------------|------|--------------|-------------|--------|--------------------------|---------------------|---------------------|----------|-------------------------|--------------------|---------------------|---------------------|----------------------|--------------------|
| 41e9dcbd-01f8-4e8f-8923-3f36a94d626d | 41e9dcbd-01f8-4e8f-8923-3f36a94d626d |    3 | Clara        | Schmid      | Basel  | clara.schmid@newmail.com | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 | ACTIVE   | CHANGED                 | UPDATE_EXISTING    | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 |                      |                    |
|                                      | 41e9dcbd-01f8-4e8f-8923-3f36a94d626d |    3 | Clara        | Schmid      | Basel  | clara.schmid@newmail.com | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 | ACTIVE   | CHANGED                 | INSERT_NEW_VERSION | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 |                      |                    |



**Dimensional Table `dim_person`**


| dp_key                                                                  | id                                   | first_name                               | last_name                                 | city                                     | email                                                       | dp_valid_from                                          | dp_valid_to                                             | dp_is_active                              | dp_is_latest                              | dp_created_at                                          | dp_replaced_at                                          |
|-------------------------------------------------------------------------|--------------------------------------|------------------------------------------|-------------------------------------------|------------------------------------------|-------------------------------------------------------------|--------------------------------------------------------|---------------------------------------------------------|-------------------------------------------|-------------------------------------------|--------------------------------------------------------|---------------------------------------------------------|
| b87d27fb-781d-4eb3-a050-d861fc06414d                                    | 1                                    | Alice                                    | Meyer                                     | Zurich                                   | alice.meyer@example.com                                     | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                     | True                                      | True                                      | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                     |
| 9154b368-b9ea-4127-a574-e847dfeea11f                                    | 2                                    | Bob                                      | Keller                                    | Bern                                     | bob.keller@example.com                                      | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                     | True                                      | True                                      | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                     |
| 41e9dcbd-01f8-4e8f-8923-3f36a94d626d                                    | 3                                    | Clara                                    | Schmid                                    | Basel                                    | clara.schmid@example.com                                    | 2026-01-01 00:00:00                                    | <span style='color: orange;'>2026-01-04 23:59:59</span> | <span style='color: orange;'>False</span> | <span style='color: orange;'>False</span> | 2026-01-02 00:00:00                                    | <span style='color: orange;'>2026-01-06 00:00:00</span> |
| <span style='color: green;'>1e92feea-d18c-4d24-8b7a-0d015f9493c3</span> | <span style='color: green;'>3</span> | <span style='color: green;'>Clara</span> | <span style='color: green;'>Schmid</span> | <span style='color: green;'>Basel</span> | <span style='color: green;'>clara.schmid@newmail.com</span> | <span style='color: green;'>2026-01-05 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  | <span style='color: green;'>True</span>   | <span style='color: green;'>True</span>   | <span style='color: green;'>2026-01-06 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  |

_the following columns where excluded from the result: `record_hash, dp_load_timestamp, change_type`_

## Test Step 2
At 2026-01-10 00:00:00, delete entity with `id=3` from raw table (physical delete) and perform SCD2 merge. The active version of the entity with `id=3` should be marked as DELETED with `dp_valid_to` = current load timestamp - 1 second and `dp_is_active` = False.


**Raw Table `raw_person`**


|   id | first_name   | last_name   | city   | email                    | status   | dp_valid_from       | dp_loaded_at        |
|------|--------------|-------------|--------|--------------------------|----------|---------------------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-01 00:00:00 |
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@newmail.com | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-10 00:00:00 | 2026-01-10 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-10 00:00:00 | 2026-01-10 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | INACTIVE | 2026-01-10 00:00:00 | 2026-01-10 00:00:00 |



**Input to Merge**


| merge_key                            | dp_key                               |   id | first_name   | last_name   | city   | email                    | src_dp_valid_from   | load_ts             | status   | change_classification   | operation_type   | tgt_dp_valid_from   | tgt_dp_valid_to     | prev_dp_valid_from   | prev_dp_valid_to    |
|--------------------------------------|--------------------------------------|------|--------------|-------------|--------|--------------------------|---------------------|---------------------|----------|-------------------------|------------------|---------------------|---------------------|----------------------|---------------------|
| 1e92feea-d18c-4d24-8b7a-0d015f9493c3 | 1e92feea-d18c-4d24-8b7a-0d015f9493c3 |    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | 2026-01-10 00:00:00 | 2026-01-10 00:00:00 | INACTIVE | DELETED                 | UPDATE_EXISTING  | 2026-01-05 00:00:00 | 9999-12-31 23:59:59 | 2026-01-01 00:00:00  | 2026-01-04 23:59:59 |



**Dimensional Table `dim_person`**


| dp_key                               |   id | first_name   | last_name   | city   | email                    | dp_valid_from       | dp_valid_to                                             | dp_is_active                              | dp_is_latest   | dp_created_at       | dp_replaced_at                                          |
|--------------------------------------|------|--------------|-------------|--------|--------------------------|---------------------|---------------------------------------------------------|-------------------------------------------|----------------|---------------------|---------------------------------------------------------|
| b87d27fb-781d-4eb3-a050-d861fc06414d |    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | 2026-01-01 00:00:00 | 9999-12-31 23:59:59                                     | True                                      | True           | 2026-01-02 00:00:00 | 9999-12-31 23:59:59                                     |
| 9154b368-b9ea-4127-a574-e847dfeea11f |    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | 2026-01-01 00:00:00 | 9999-12-31 23:59:59                                     | True                                      | True           | 2026-01-02 00:00:00 | 9999-12-31 23:59:59                                     |
| 41e9dcbd-01f8-4e8f-8923-3f36a94d626d |    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | 2026-01-01 00:00:00 | 2026-01-04 23:59:59                                     | False                                     | False          | 2026-01-02 00:00:00 | 2026-01-06 00:00:00                                     |
| 1e92feea-d18c-4d24-8b7a-0d015f9493c3 |    3 | Clara        | Schmid      | Basel  | clara.schmid@newmail.com | 2026-01-05 00:00:00 | <span style='color: orange;'>2026-01-09 23:59:59</span> | <span style='color: orange;'>False</span> | True           | 2026-01-06 00:00:00 | <span style='color: orange;'>2026-01-11 00:00:00</span> |

_the following columns where excluded from the result: `record_hash, dp_load_timestamp, change_type`_

