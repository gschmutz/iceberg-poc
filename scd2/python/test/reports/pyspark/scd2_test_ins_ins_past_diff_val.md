# Testing Insert Operation followed by an Insert Operation in the past

This test validates an INSERT operation in the past of a version for an entity which already exists and with a different value.


 * **Strategy:** `pyspark`
 * **Last Run:** `2026-04-10 14:24:15`
## Test Step 1
At 2026-01-01 00:00:00, insert 3 entities into raw table and perform initial SCD2 merge.


**Raw Table `raw_person`**


|   id | first_name   | last_name   | city   | email                    | status   | dp_ts_from          | dp_loaded_at        |
|------|--------------|-------------|--------|--------------------------|----------|---------------------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |



**Input to Merge**


| merge_key   | dp_key                               |   id | first_name   | last_name   | city   | email                    | record_hash                                                      | load_ts             | status   | operation_type     | case_name   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   |
|-------------|--------------------------------------|------|--------------|-------------|--------|--------------------------|------------------------------------------------------------------|---------------------|----------|--------------------|-------------|---------------------|---------------------|----------------|----------------|
|             | 0f4046ff-3e2f-4c00-8e05-c102b99e998e |    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | 00B9A7122065F01BE7FD23C6FB962AEE6DE3B84D0BA50409DC26FC5A150FBDC8 | 2026-01-05 00:00:00 | ACTIVE   | INSERT_NEW_VERSION | CASE_1      | 2026-01-05 00:00:00 | 9999-12-31 23:59:59 | True           | True           |
|             | d552770d-9136-453a-8132-beacfa3e699e |    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | D28A23C8422275E006FCF3D86AA51CF4E058FB495B8E48560FC9BF7BCC019B40 | 2026-01-05 00:00:00 | ACTIVE   | INSERT_NEW_VERSION | CASE_1      | 2026-01-05 00:00:00 | 9999-12-31 23:59:59 | True           | True           |
|             | 4df0d9f8-d419-4561-9941-58b3cd136ff5 |    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | 77C069EE2AA3730894A6E3319ADC455C203B6CC4D35B0B912C2FAADF3C687676 | 2026-01-05 00:00:00 | ACTIVE   | INSERT_NEW_VERSION | CASE_1      | 2026-01-05 00:00:00 | 9999-12-31 23:59:59 | True           | True           |



**Dimensional Table `dim_person`**


| dp_key                                                                  | id                                   | first_name                               | last_name                                 | city                                      | email                                                       | dp_ts_from                                             | dp_ts_to                                               | dp_is_active                            | dp_is_latest                            | dp_created_at                                          | dp_replaced_at                                         |
|-------------------------------------------------------------------------|--------------------------------------|------------------------------------------|-------------------------------------------|-------------------------------------------|-------------------------------------------------------------|--------------------------------------------------------|--------------------------------------------------------|-----------------------------------------|-----------------------------------------|--------------------------------------------------------|--------------------------------------------------------|
| <span style='color: green;'>0f4046ff-3e2f-4c00-8e05-c102b99e998e</span> | <span style='color: green;'>1</span> | <span style='color: green;'>Alice</span> | <span style='color: green;'>Meyer</span>  | <span style='color: green;'>Zurich</span> | <span style='color: green;'>alice.meyer@example.com</span>  | <span style='color: green;'>2026-01-05 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-06 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>d552770d-9136-453a-8132-beacfa3e699e</span> | <span style='color: green;'>2</span> | <span style='color: green;'>Bob</span>   | <span style='color: green;'>Keller</span> | <span style='color: green;'>Bern</span>   | <span style='color: green;'>bob.keller@example.com</span>   | <span style='color: green;'>2026-01-05 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-06 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>4df0d9f8-d419-4561-9941-58b3cd136ff5</span> | <span style='color: green;'>3</span> | <span style='color: green;'>Clara</span> | <span style='color: green;'>Schmid</span> | <span style='color: green;'>Basel</span>  | <span style='color: green;'>clara.schmid@example.com</span> | <span style='color: green;'>2026-01-05 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-06 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |

_the following columns where excluded from the result: `record_hash, dp_load_timestamp, change_type`_

## Test Step 2
At 2026-01-01 00:00:00, insert the entity with `id=1` with a different value for `city` into the new partitions of the raw table and perform SCD2 merge.


**Raw Table `raw_person`**


|   id | first_name   | last_name   | city   | email                    | status   | dp_ts_from          | dp_loaded_at        |
|------|--------------|-------------|--------|--------------------------|----------|---------------------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    1 | Alice        | Meyer       | Geneva | alice.meyer@example.com  | ACTIVE   | 2026-01-01 00:00:00 | 2026-01-10 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-10 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-10 00:00:00 |



**Input to Merge**


| merge_key   | dp_key                               |   id | first_name   | last_name   | city   | email                   | record_hash                                                      | load_ts             | status   | operation_type     | case_name   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   |
|-------------|--------------------------------------|------|--------------|-------------|--------|-------------------------|------------------------------------------------------------------|---------------------|----------|--------------------|-------------|---------------------|---------------------|----------------|----------------|
|             | 7a0692fe-b91f-44bc-8077-a3c79a44e9c3 |    1 | Alice        | Meyer       | Geneva | alice.meyer@example.com | 78FFEBE2007761B0577842A5487D3B5327964306AA3FE61834D60464D0D4AF8F | 2026-01-10 00:00:00 | ACTIVE   | INSERT_NEW_VERSION | CASE_19     | 2026-01-01 00:00:00 | 2026-01-04 23:59:59 | False          | False          |



**Dimensional Table `dim_person`**


| dp_key                                                                  | id                                   | first_name                               | last_name                                | city                                      | email                                                      | dp_ts_from                                             | dp_ts_to                                               | dp_is_active                             | dp_is_latest                             | dp_created_at                                          | dp_replaced_at                                         |
|-------------------------------------------------------------------------|--------------------------------------|------------------------------------------|------------------------------------------|-------------------------------------------|------------------------------------------------------------|--------------------------------------------------------|--------------------------------------------------------|------------------------------------------|------------------------------------------|--------------------------------------------------------|--------------------------------------------------------|
| <span style='color: green;'>7a0692fe-b91f-44bc-8077-a3c79a44e9c3</span> | <span style='color: green;'>1</span> | <span style='color: green;'>Alice</span> | <span style='color: green;'>Meyer</span> | <span style='color: green;'>Geneva</span> | <span style='color: green;'>alice.meyer@example.com</span> | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>2026-01-04 23:59:59</span> | <span style='color: green;'>False</span> | <span style='color: green;'>False</span> | <span style='color: green;'>2026-01-11 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| 0f4046ff-3e2f-4c00-8e05-c102b99e998e                                    | 1                                    | Alice                                    | Meyer                                    | Zurich                                    | alice.meyer@example.com                                    | 2026-01-05 00:00:00                                    | 9999-12-31 23:59:59                                    | True                                     | True                                     | 2026-01-06 00:00:00                                    | 9999-12-31 23:59:59                                    |
| d552770d-9136-453a-8132-beacfa3e699e                                    | 2                                    | Bob                                      | Keller                                   | Bern                                      | bob.keller@example.com                                     | 2026-01-05 00:00:00                                    | 9999-12-31 23:59:59                                    | True                                     | True                                     | 2026-01-06 00:00:00                                    | 9999-12-31 23:59:59                                    |
| 4df0d9f8-d419-4561-9941-58b3cd136ff5                                    | 3                                    | Clara                                    | Schmid                                   | Basel                                     | clara.schmid@example.com                                   | 2026-01-05 00:00:00                                    | 9999-12-31 23:59:59                                    | True                                     | True                                     | 2026-01-06 00:00:00                                    | 9999-12-31 23:59:59                                    |

_the following columns where excluded from the result: `record_hash, dp_load_timestamp, change_type`_

