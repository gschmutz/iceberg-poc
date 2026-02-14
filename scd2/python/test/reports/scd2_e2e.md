# End-to-End SCD2 Test Case over multiple days

This test performs SCD2 operations over 5 days.
## Test Step 1
Insert 3 records into raw table and perform initial SCD2 merge.


**Raw Table `raw_person`**


|   id | first_name   | last_name   | city   | email                    | status   | dp_loaded_at        |
|------|--------------|-------------|--------|--------------------------|----------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-01 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-01 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-01 00:00:00 |



**Input to Merge**


|   merge_key |   id | first_name   | last_name   | city   | email                    | load_ts             | status   | change_classification   | operation_type   | tgt_dp_valid_from   | tgt_dp_valid_to     |
|-------------|------|--------------|-------------|--------|--------------------------|---------------------|----------|-------------------------|------------------|---------------------|---------------------|
|           1 |    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | 2026-01-01 00:00:00 | ACTIVE   | NEW                     | UPDATE_EXISTING  | 9999-12-31 23:59:59 | 9999-12-31 23:59:59 |
|           2 |    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | 2026-01-01 00:00:00 | ACTIVE   | NEW                     | UPDATE_EXISTING  | 9999-12-31 23:59:59 | 9999-12-31 23:59:59 |
|           3 |    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | 2026-01-01 00:00:00 | ACTIVE   | NEW                     | UPDATE_EXISTING  | 9999-12-31 23:59:59 | 9999-12-31 23:59:59 |



**Dimensional Table `dim_person`**


| id                                   | first_name                               | last_name                                 | city                                      | email                                                       | dp_valid_from                                          | dp_valid_to                                            | dp_is_active                            | dp_is_latest                            | dp_created_at                                          | dp_replaced_at                                         |
|--------------------------------------|------------------------------------------|-------------------------------------------|-------------------------------------------|-------------------------------------------------------------|--------------------------------------------------------|--------------------------------------------------------|-----------------------------------------|-----------------------------------------|--------------------------------------------------------|--------------------------------------------------------|
| <span style='color: green;'>1</span> | <span style='color: green;'>Alice</span> | <span style='color: green;'>Meyer</span>  | <span style='color: green;'>Zurich</span> | <span style='color: green;'>alice.meyer@example.com</span>  | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>2</span> | <span style='color: green;'>Bob</span>   | <span style='color: green;'>Keller</span> | <span style='color: green;'>Bern</span>   | <span style='color: green;'>bob.keller@example.com</span>   | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>3</span> | <span style='color: green;'>Clara</span> | <span style='color: green;'>Schmid</span> | <span style='color: green;'>Basel</span>  | <span style='color: green;'>clara.schmid@example.com</span> | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-02 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |

_the following columns where excluded from the result: `record_hash, dp_load_timestamp, change_type`_

## Test Step 2
* update City for Alice
* update Email for Clara
* Insert Kevin
*  Insert Laura


**Raw Table `raw_person`**


|   id | first_name   | last_name   | city   | email                    | status   | dp_loaded_at        |
|------|--------------|-------------|--------|--------------------------|----------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-01 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-01 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-01 00:00:00 |
|    1 | Alice        | Meyer       | Bern   | alice.meyer@example.com  | ACTIVE   | 2026-01-05 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-05 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@newmail.com | ACTIVE   | 2026-01-05 00:00:00 |
|   10 | Kevin        | Loosli      | Bern   | kevin.loosli@example.com | ACTIVE   | 2026-01-05 00:00:00 |
|   11 | Laura        | Graf        | Basel  | laura.graf@example.com   | ACTIVE   | 2026-01-05 00:00:00 |



**Input to Merge**


|   merge_key |   id | first_name   | last_name   | city   | email                    | load_ts             | status   | change_classification   | operation_type     | tgt_dp_valid_from   | tgt_dp_valid_to     |
|-------------|------|--------------|-------------|--------|--------------------------|---------------------|----------|-------------------------|--------------------|---------------------|---------------------|
|           1 |    1 | Alice        | Meyer       | Bern   | alice.meyer@example.com  | 2026-01-05 00:00:00 | ACTIVE   | CHANGED                 | UPDATE_EXISTING    | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 |
|           3 |    3 | Clara        | Schmid      | Basel  | clara.schmid@newmail.com | 2026-01-05 00:00:00 | ACTIVE   | CHANGED                 | UPDATE_EXISTING    | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 |
|          10 |   10 | Kevin        | Loosli      | Bern   | kevin.loosli@example.com | 2026-01-05 00:00:00 | ACTIVE   | NEW                     | UPDATE_EXISTING    | 9999-12-31 23:59:59 | 9999-12-31 23:59:59 |
|          11 |   11 | Laura        | Graf        | Basel  | laura.graf@example.com   | 2026-01-05 00:00:00 | ACTIVE   | NEW                     | UPDATE_EXISTING    | 9999-12-31 23:59:59 | 9999-12-31 23:59:59 |
|         nan |    1 | Alice        | Meyer       | Bern   | alice.meyer@example.com  | 2026-01-05 00:00:00 | ACTIVE   | CHANGED                 | INSERT_NEW_VERSION | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 |
|         nan |    3 | Clara        | Schmid      | Basel  | clara.schmid@newmail.com | 2026-01-05 00:00:00 | ACTIVE   | CHANGED                 | INSERT_NEW_VERSION | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 |



**Dimensional Table `dim_person`**


| id                                    | first_name                               | last_name                                 | city                                     | email                                                       | dp_valid_from                                          | dp_valid_to                                             | dp_is_active                              | dp_is_latest                              | dp_created_at                                          | dp_replaced_at                                          |
|---------------------------------------|------------------------------------------|-------------------------------------------|------------------------------------------|-------------------------------------------------------------|--------------------------------------------------------|---------------------------------------------------------|-------------------------------------------|-------------------------------------------|--------------------------------------------------------|---------------------------------------------------------|
| 1                                     | Alice                                    | Meyer                                     | Zurich                                   | alice.meyer@example.com                                     | 2026-01-01 00:00:00                                    | <span style='color: orange;'>2026-01-04 23:59:59</span> | <span style='color: orange;'>False</span> | <span style='color: orange;'>False</span> | 2026-01-02 00:00:00                                    | <span style='color: orange;'>2026-01-06 00:00:00</span> |
| <span style='color: green;'>1</span>  | <span style='color: green;'>Alice</span> | <span style='color: green;'>Meyer</span>  | <span style='color: green;'>Bern</span>  | <span style='color: green;'>alice.meyer@example.com</span>  | <span style='color: green;'>2026-01-05 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  | <span style='color: green;'>True</span>   | <span style='color: green;'>True</span>   | <span style='color: green;'>2026-01-06 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  |
| 2                                     | Bob                                      | Keller                                    | Bern                                     | bob.keller@example.com                                      | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                     | True                                      | True                                      | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                     |
| 3                                     | Clara                                    | Schmid                                    | Basel                                    | clara.schmid@example.com                                    | 2026-01-01 00:00:00                                    | <span style='color: orange;'>2026-01-04 23:59:59</span> | <span style='color: orange;'>False</span> | <span style='color: orange;'>False</span> | 2026-01-02 00:00:00                                    | <span style='color: orange;'>2026-01-06 00:00:00</span> |
| <span style='color: green;'>3</span>  | <span style='color: green;'>Clara</span> | <span style='color: green;'>Schmid</span> | <span style='color: green;'>Basel</span> | <span style='color: green;'>clara.schmid@newmail.com</span> | <span style='color: green;'>2026-01-05 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  | <span style='color: green;'>True</span>   | <span style='color: green;'>True</span>   | <span style='color: green;'>2026-01-06 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  |
| <span style='color: green;'>10</span> | <span style='color: green;'>Kevin</span> | <span style='color: green;'>Loosli</span> | <span style='color: green;'>Bern</span>  | <span style='color: green;'>kevin.loosli@example.com</span> | <span style='color: green;'>2026-01-05 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  | <span style='color: green;'>True</span>   | <span style='color: green;'>True</span>   | <span style='color: green;'>2026-01-06 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  |
| <span style='color: green;'>11</span> | <span style='color: green;'>Laura</span> | <span style='color: green;'>Graf</span>   | <span style='color: green;'>Basel</span> | <span style='color: green;'>laura.graf@example.com</span>   | <span style='color: green;'>2026-01-05 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  | <span style='color: green;'>True</span>   | <span style='color: green;'>True</span>   | <span style='color: green;'>2026-01-06 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  |

_the following columns where excluded from the result: `record_hash, dp_load_timestamp, change_type`_

## Test Step 3
* update Email for Alice
* update Email for Laura


**Raw Table `raw_person`**


|   id | first_name   | last_name   | city   | email                    | status   | dp_loaded_at        |
|------|--------------|-------------|--------|--------------------------|----------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-01 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-01 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-01 00:00:00 |
|    1 | Alice        | Meyer       | Bern   | alice.meyer@example.com  | ACTIVE   | 2026-01-05 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-05 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@newmail.com | ACTIVE   | 2026-01-05 00:00:00 |
|   10 | Kevin        | Loosli      | Bern   | kevin.loosli@example.com | ACTIVE   | 2026-01-05 00:00:00 |
|   11 | Laura        | Graf        | Basel  | laura.graf@example.com   | ACTIVE   | 2026-01-05 00:00:00 |
|    1 | Alice        | Meyer       | Bern   | alice.meyer@newmail.com  | ACTIVE   | 2026-01-15 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-15 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@newmail.com | ACTIVE   | 2026-01-15 00:00:00 |
|   10 | Kevin        | Loosli      | Bern   | kevin.loosli@example.com | ACTIVE   | 2026-01-15 00:00:00 |
|   11 | Laura        | Graf        | Basel  | laura.graf@newmail.com   | ACTIVE   | 2026-01-15 00:00:00 |



**Input to Merge**


|   merge_key |   id | first_name   | last_name   | city   | email                   | load_ts             | status   | change_classification   | operation_type     | tgt_dp_valid_from   | tgt_dp_valid_to     |
|-------------|------|--------------|-------------|--------|-------------------------|---------------------|----------|-------------------------|--------------------|---------------------|---------------------|
|           1 |    1 | Alice        | Meyer       | Bern   | alice.meyer@newmail.com | 2026-01-15 00:00:00 | ACTIVE   | CHANGED                 | UPDATE_EXISTING    | 2026-01-05 00:00:00 | 9999-12-31 23:59:59 |
|          11 |   11 | Laura        | Graf        | Basel  | laura.graf@newmail.com  | 2026-01-15 00:00:00 | ACTIVE   | CHANGED                 | UPDATE_EXISTING    | 2026-01-05 00:00:00 | 9999-12-31 23:59:59 |
|         nan |    1 | Alice        | Meyer       | Bern   | alice.meyer@newmail.com | 2026-01-15 00:00:00 | ACTIVE   | CHANGED                 | INSERT_NEW_VERSION | 2026-01-05 00:00:00 | 9999-12-31 23:59:59 |
|         nan |   11 | Laura        | Graf        | Basel  | laura.graf@newmail.com  | 2026-01-15 00:00:00 | ACTIVE   | CHANGED                 | INSERT_NEW_VERSION | 2026-01-05 00:00:00 | 9999-12-31 23:59:59 |



**Dimensional Table `dim_person`**


| id                                    | first_name                               | last_name                                | city                                     | email                                                      | dp_valid_from                                          | dp_valid_to                                             | dp_is_active                              | dp_is_latest                              | dp_created_at                                          | dp_replaced_at                                          |
|---------------------------------------|------------------------------------------|------------------------------------------|------------------------------------------|------------------------------------------------------------|--------------------------------------------------------|---------------------------------------------------------|-------------------------------------------|-------------------------------------------|--------------------------------------------------------|---------------------------------------------------------|
| 1                                     | Alice                                    | Meyer                                    | Zurich                                   | alice.meyer@example.com                                    | 2026-01-01 00:00:00                                    | 2026-01-04 23:59:59                                     | False                                     | False                                     | 2026-01-02 00:00:00                                    | 2026-01-06 00:00:00                                     |
| 1                                     | Alice                                    | Meyer                                    | Bern                                     | alice.meyer@example.com                                    | 2026-01-05 00:00:00                                    | <span style='color: orange;'>2026-01-14 23:59:59</span> | <span style='color: orange;'>False</span> | <span style='color: orange;'>False</span> | 2026-01-06 00:00:00                                    | <span style='color: orange;'>2026-01-16 00:00:00</span> |
| <span style='color: green;'>1</span>  | <span style='color: green;'>Alice</span> | <span style='color: green;'>Meyer</span> | <span style='color: green;'>Bern</span>  | <span style='color: green;'>alice.meyer@newmail.com</span> | <span style='color: green;'>2026-01-15 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  | <span style='color: green;'>True</span>   | <span style='color: green;'>True</span>   | <span style='color: green;'>2026-01-16 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  |
| 2                                     | Bob                                      | Keller                                   | Bern                                     | bob.keller@example.com                                     | 2026-01-01 00:00:00                                    | 9999-12-31 23:59:59                                     | True                                      | True                                      | 2026-01-02 00:00:00                                    | 9999-12-31 23:59:59                                     |
| 3                                     | Clara                                    | Schmid                                   | Basel                                    | clara.schmid@example.com                                   | 2026-01-01 00:00:00                                    | 2026-01-04 23:59:59                                     | False                                     | False                                     | 2026-01-02 00:00:00                                    | 2026-01-06 00:00:00                                     |
| 3                                     | Clara                                    | Schmid                                   | Basel                                    | clara.schmid@newmail.com                                   | 2026-01-05 00:00:00                                    | 9999-12-31 23:59:59                                     | True                                      | True                                      | 2026-01-06 00:00:00                                    | 9999-12-31 23:59:59                                     |
| 10                                    | Kevin                                    | Loosli                                   | Bern                                     | kevin.loosli@example.com                                   | 2026-01-05 00:00:00                                    | 9999-12-31 23:59:59                                     | True                                      | True                                      | 2026-01-06 00:00:00                                    | 9999-12-31 23:59:59                                     |
| 11                                    | Laura                                    | Graf                                     | Basel                                    | laura.graf@example.com                                     | 2026-01-05 00:00:00                                    | <span style='color: orange;'>2026-01-14 23:59:59</span> | <span style='color: orange;'>False</span> | <span style='color: orange;'>False</span> | 2026-01-06 00:00:00                                    | <span style='color: orange;'>2026-01-16 00:00:00</span> |
| <span style='color: green;'>11</span> | <span style='color: green;'>Laura</span> | <span style='color: green;'>Graf</span>  | <span style='color: green;'>Basel</span> | <span style='color: green;'>laura.graf@newmail.com</span>  | <span style='color: green;'>2026-01-15 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  | <span style='color: green;'>True</span>   | <span style='color: green;'>True</span>   | <span style='color: green;'>2026-01-16 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span>  |

_the following columns where excluded from the result: `record_hash, dp_load_timestamp, change_type`_

## Test Step 4
* delete Bob
* delete Kevin


**Raw Table `raw_person`**


|   id | first_name   | last_name   | city   | email                    | status   | dp_loaded_at        |
|------|--------------|-------------|--------|--------------------------|----------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-01 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-01 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-01 00:00:00 |
|    1 | Alice        | Meyer       | Bern   | alice.meyer@example.com  | ACTIVE   | 2026-01-05 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-05 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@newmail.com | ACTIVE   | 2026-01-05 00:00:00 |
|   10 | Kevin        | Loosli      | Bern   | kevin.loosli@example.com | ACTIVE   | 2026-01-05 00:00:00 |
|   11 | Laura        | Graf        | Basel  | laura.graf@example.com   | ACTIVE   | 2026-01-05 00:00:00 |
|    1 | Alice        | Meyer       | Bern   | alice.meyer@newmail.com  | ACTIVE   | 2026-01-15 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-15 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@newmail.com | ACTIVE   | 2026-01-15 00:00:00 |
|   10 | Kevin        | Loosli      | Bern   | kevin.loosli@example.com | ACTIVE   | 2026-01-15 00:00:00 |
|   11 | Laura        | Graf        | Basel  | laura.graf@newmail.com   | ACTIVE   | 2026-01-15 00:00:00 |
|    1 | Alice        | Meyer       | Bern   | alice.meyer@newmail.com  | ACTIVE   | 2026-02-10 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | INACTIVE | 2026-02-10 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@newmail.com | ACTIVE   | 2026-02-10 00:00:00 |
|   10 | Kevin        | Loosli      | Bern   | kevin.loosli@example.com | INACTIVE | 2026-02-10 00:00:00 |
|   11 | Laura        | Graf        | Basel  | laura.graf@newmail.com   | ACTIVE   | 2026-02-10 00:00:00 |



**Input to Merge**


|   merge_key |   id | first_name   | last_name   | city   | email                    | load_ts             | status   | change_classification   | operation_type   | tgt_dp_valid_from   | tgt_dp_valid_to     |
|-------------|------|--------------|-------------|--------|--------------------------|---------------------|----------|-------------------------|------------------|---------------------|---------------------|
|           2 |    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | 2026-02-10 00:00:00 | INACTIVE | DELETED                 | UPDATE_EXISTING  | 2026-01-01 00:00:00 | 9999-12-31 23:59:59 |
|          10 |   10 | Kevin        | Loosli      | Bern   | kevin.loosli@example.com | 2026-02-10 00:00:00 | INACTIVE | DELETED                 | UPDATE_EXISTING  | 2026-01-05 00:00:00 | 9999-12-31 23:59:59 |



**Dimensional Table `dim_person`**


|   id | first_name   | last_name   | city   | email                    | dp_valid_from       | dp_valid_to                                             | dp_is_active                              | dp_is_latest   | dp_created_at       | dp_replaced_at                                          |
|------|--------------|-------------|--------|--------------------------|---------------------|---------------------------------------------------------|-------------------------------------------|----------------|---------------------|---------------------------------------------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | 2026-01-01 00:00:00 | 2026-01-04 23:59:59                                     | False                                     | False          | 2026-01-02 00:00:00 | 2026-01-06 00:00:00                                     |
|    1 | Alice        | Meyer       | Bern   | alice.meyer@example.com  | 2026-01-05 00:00:00 | 2026-01-14 23:59:59                                     | False                                     | False          | 2026-01-06 00:00:00 | 2026-01-16 00:00:00                                     |
|    1 | Alice        | Meyer       | Bern   | alice.meyer@newmail.com  | 2026-01-15 00:00:00 | 9999-12-31 23:59:59                                     | True                                      | True           | 2026-01-16 00:00:00 | 9999-12-31 23:59:59                                     |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | 2026-01-01 00:00:00 | <span style='color: orange;'>2026-02-09 23:59:59</span> | <span style='color: orange;'>False</span> | True           | 2026-01-02 00:00:00 | <span style='color: orange;'>2026-02-11 00:00:00</span> |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | 2026-01-01 00:00:00 | 2026-01-04 23:59:59                                     | False                                     | False          | 2026-01-02 00:00:00 | 2026-01-06 00:00:00                                     |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@newmail.com | 2026-01-05 00:00:00 | 9999-12-31 23:59:59                                     | True                                      | True           | 2026-01-06 00:00:00 | 9999-12-31 23:59:59                                     |
|   10 | Kevin        | Loosli      | Bern   | kevin.loosli@example.com | 2026-01-05 00:00:00 | <span style='color: orange;'>2026-02-09 23:59:59</span> | <span style='color: orange;'>False</span> | True           | 2026-01-06 00:00:00 | <span style='color: orange;'>2026-02-11 00:00:00</span> |
|   11 | Laura        | Graf        | Basel  | laura.graf@example.com   | 2026-01-05 00:00:00 | 2026-01-14 23:59:59                                     | False                                     | False          | 2026-01-06 00:00:00 | 2026-01-16 00:00:00                                     |
|   11 | Laura        | Graf        | Basel  | laura.graf@newmail.com   | 2026-01-15 00:00:00 | 9999-12-31 23:59:59                                     | True                                      | True           | 2026-01-16 00:00:00 | 9999-12-31 23:59:59                                     |

_the following columns where excluded from the result: `record_hash, dp_load_timestamp, change_type`_

## Test Step 5
* reactivate Kevin
* delete Markus


**Raw Table `raw_person`**


|   id | first_name   | last_name   | city    | email                      | status   | dp_loaded_at        |
|------|--------------|-------------|---------|----------------------------|----------|---------------------|
|    1 | Alice        | Meyer       | Zurich  | alice.meyer@example.com    | ACTIVE   | 2026-01-01 00:00:00 |
|    2 | Bob          | Keller      | Bern    | bob.keller@example.com     | ACTIVE   | 2026-01-01 00:00:00 |
|    3 | Clara        | Schmid      | Basel   | clara.schmid@example.com   | ACTIVE   | 2026-01-01 00:00:00 |
|    1 | Alice        | Meyer       | Bern    | alice.meyer@example.com    | ACTIVE   | 2026-01-05 00:00:00 |
|    2 | Bob          | Keller      | Bern    | bob.keller@example.com     | ACTIVE   | 2026-01-05 00:00:00 |
|    3 | Clara        | Schmid      | Basel   | clara.schmid@newmail.com   | ACTIVE   | 2026-01-05 00:00:00 |
|   10 | Kevin        | Loosli      | Bern    | kevin.loosli@example.com   | ACTIVE   | 2026-01-05 00:00:00 |
|   11 | Laura        | Graf        | Basel   | laura.graf@example.com     | ACTIVE   | 2026-01-05 00:00:00 |
|    1 | Alice        | Meyer       | Bern    | alice.meyer@newmail.com    | ACTIVE   | 2026-01-15 00:00:00 |
|    2 | Bob          | Keller      | Bern    | bob.keller@example.com     | ACTIVE   | 2026-01-15 00:00:00 |
|    3 | Clara        | Schmid      | Basel   | clara.schmid@newmail.com   | ACTIVE   | 2026-01-15 00:00:00 |
|   10 | Kevin        | Loosli      | Bern    | kevin.loosli@example.com   | ACTIVE   | 2026-01-15 00:00:00 |
|   11 | Laura        | Graf        | Basel   | laura.graf@newmail.com     | ACTIVE   | 2026-01-15 00:00:00 |
|    1 | Alice        | Meyer       | Bern    | alice.meyer@newmail.com    | ACTIVE   | 2026-02-10 00:00:00 |
|    2 | Bob          | Keller      | Bern    | bob.keller@example.com     | INACTIVE | 2026-02-10 00:00:00 |
|    3 | Clara        | Schmid      | Basel   | clara.schmid@newmail.com   | ACTIVE   | 2026-02-10 00:00:00 |
|   10 | Kevin        | Loosli      | Bern    | kevin.loosli@example.com   | INACTIVE | 2026-02-10 00:00:00 |
|   11 | Laura        | Graf        | Basel   | laura.graf@newmail.com     | ACTIVE   | 2026-02-10 00:00:00 |
|    1 | Alice        | Meyer       | Bern    | alice.meyer@newmail.com    | ACTIVE   | 2026-02-20 00:00:00 |
|    2 | Bob          | Keller      | Bern    | bob.keller@example.com     | INACTIVE | 2026-02-20 00:00:00 |
|    3 | Clara        | Schmid      | Basel   | clara.schmid@newmail.com   | ACTIVE   | 2026-02-20 00:00:00 |
|   10 | Kevin        | Loosli      | Bern    | kevin.loosli@example.com   | ACTIVE   | 2026-02-20 00:00:00 |
|   11 | Laura        | Graf        | Basel   | laura.graf@newmail.com     | ACTIVE   | 2026-02-20 00:00:00 |
|   12 | Markus       | Steiner     | Lucerne | markus.steiner@example.com | ACTIVE   | 2026-02-20 00:00:00 |



**Input to Merge**


|   merge_key |   id | first_name   | last_name   | city    | email                      | load_ts             | status   | change_classification   | operation_type   | tgt_dp_valid_from   | tgt_dp_valid_to     |
|-------------|------|--------------|-------------|---------|----------------------------|---------------------|----------|-------------------------|------------------|---------------------|---------------------|
|           2 |    2 | Bob          | Keller      | Bern    | bob.keller@example.com     | 2026-02-20 00:00:00 | INACTIVE | NEW                     | UPDATE_EXISTING  | 9999-12-31 23:59:59 | 9999-12-31 23:59:59 |
|          10 |   10 | Kevin        | Loosli      | Bern    | kevin.loosli@example.com   | 2026-02-20 00:00:00 | ACTIVE   | NEW                     | UPDATE_EXISTING  | 9999-12-31 23:59:59 | 9999-12-31 23:59:59 |
|          12 |   12 | Markus       | Steiner     | Lucerne | markus.steiner@example.com | 2026-02-20 00:00:00 | ACTIVE   | NEW                     | UPDATE_EXISTING  | 9999-12-31 23:59:59 | 9999-12-31 23:59:59 |



**Dimensional Table `dim_person`**


| id                                    | first_name                                | last_name                                  | city                                       | email                                                         | dp_valid_from                                          | dp_valid_to                                            | dp_is_active                            | dp_is_latest                            | dp_created_at                                          | dp_replaced_at                                         |
|---------------------------------------|-------------------------------------------|--------------------------------------------|--------------------------------------------|---------------------------------------------------------------|--------------------------------------------------------|--------------------------------------------------------|-----------------------------------------|-----------------------------------------|--------------------------------------------------------|--------------------------------------------------------|
| 1                                     | Alice                                     | Meyer                                      | Zurich                                     | alice.meyer@example.com                                       | 2026-01-01 00:00:00                                    | 2026-01-04 23:59:59                                    | False                                   | False                                   | 2026-01-02 00:00:00                                    | 2026-01-06 00:00:00                                    |
| 1                                     | Alice                                     | Meyer                                      | Bern                                       | alice.meyer@example.com                                       | 2026-01-05 00:00:00                                    | 2026-01-14 23:59:59                                    | False                                   | False                                   | 2026-01-06 00:00:00                                    | 2026-01-16 00:00:00                                    |
| 1                                     | Alice                                     | Meyer                                      | Bern                                       | alice.meyer@newmail.com                                       | 2026-01-15 00:00:00                                    | 9999-12-31 23:59:59                                    | True                                    | True                                    | 2026-01-16 00:00:00                                    | 9999-12-31 23:59:59                                    |
| 2                                     | Bob                                       | Keller                                     | Bern                                       | bob.keller@example.com                                        | 2026-01-01 00:00:00                                    | 2026-02-09 23:59:59                                    | False                                   | True                                    | 2026-01-02 00:00:00                                    | 2026-02-11 00:00:00                                    |
| <span style='color: green;'>2</span>  | <span style='color: green;'>Bob</span>    | <span style='color: green;'>Keller</span>  | <span style='color: green;'>Bern</span>    | <span style='color: green;'>bob.keller@example.com</span>     | <span style='color: green;'>2026-02-20 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-02-21 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| 3                                     | Clara                                     | Schmid                                     | Basel                                      | clara.schmid@example.com                                      | 2026-01-01 00:00:00                                    | 2026-01-04 23:59:59                                    | False                                   | False                                   | 2026-01-02 00:00:00                                    | 2026-01-06 00:00:00                                    |
| 3                                     | Clara                                     | Schmid                                     | Basel                                      | clara.schmid@newmail.com                                      | 2026-01-05 00:00:00                                    | 9999-12-31 23:59:59                                    | True                                    | True                                    | 2026-01-06 00:00:00                                    | 9999-12-31 23:59:59                                    |
| 10                                    | Kevin                                     | Loosli                                     | Bern                                       | kevin.loosli@example.com                                      | 2026-01-05 00:00:00                                    | 2026-02-09 23:59:59                                    | False                                   | True                                    | 2026-01-06 00:00:00                                    | 2026-02-11 00:00:00                                    |
| <span style='color: green;'>10</span> | <span style='color: green;'>Kevin</span>  | <span style='color: green;'>Loosli</span>  | <span style='color: green;'>Bern</span>    | <span style='color: green;'>kevin.loosli@example.com</span>   | <span style='color: green;'>2026-02-20 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-02-21 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| 11                                    | Laura                                     | Graf                                       | Basel                                      | laura.graf@example.com                                        | 2026-01-05 00:00:00                                    | 2026-01-14 23:59:59                                    | False                                   | False                                   | 2026-01-06 00:00:00                                    | 2026-01-16 00:00:00                                    |
| 11                                    | Laura                                     | Graf                                       | Basel                                      | laura.graf@newmail.com                                        | 2026-01-15 00:00:00                                    | 9999-12-31 23:59:59                                    | True                                    | True                                    | 2026-01-16 00:00:00                                    | 9999-12-31 23:59:59                                    |
| <span style='color: green;'>12</span> | <span style='color: green;'>Markus</span> | <span style='color: green;'>Steiner</span> | <span style='color: green;'>Lucerne</span> | <span style='color: green;'>markus.steiner@example.com</span> | <span style='color: green;'>2026-02-20 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-02-21 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |

_the following columns where excluded from the result: `record_hash, dp_load_timestamp, change_type`_

