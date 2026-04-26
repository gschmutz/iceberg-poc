# Testing Insert Operation followed by an Insert Operation in the past

This test validates an INSERT operation in the past of a version for an entity which already exists and with a different value.


 * **Strategy:** `spark`
 * **Last Run:** `2026-04-26 19:14:14`
## Test Step 1
At 2026-01-01 00:00:00, insert 3 entities into raw table and perform initial SCD2 merge.


**Raw Table `raw_person`**


|   id | first_name   | last_name   | city   | email                    | status   | dp_ts_from          | dp_loaded_at        |
|------|--------------|-------------|--------|--------------------------|----------|---------------------|---------------------|
|    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |
|    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | ACTIVE   | 2026-01-05 00:00:00 | 2026-01-05 00:00:00 |



**Input to Merge**


| merge_record_id   | dp_record_id                         |   id | first_name   | last_name   | city   | email                    | dp_record_hash                                                   | dp_ts               | status   | operation_type     | case_name   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   |
|-------------------|--------------------------------------|------|--------------|-------------|--------|--------------------------|------------------------------------------------------------------|---------------------|----------|--------------------|-------------|---------------------|---------------------|----------------|----------------|
|                   | bdfc910d-be81-4158-8f13-a52a82b8816e |    1 | Alice        | Meyer       | Zurich | alice.meyer@example.com  | 00B9A7122065F01BE7FD23C6FB962AEE6DE3B84D0BA50409DC26FC5A150FBDC8 | 2026-01-05 00:00:00 | ACTIVE   | INSERT_NEW_VERSION | CASE_1      | 2026-01-05 00:00:00 | 9999-12-31 23:59:59 | True           | True           |
|                   | 63a8c6b7-f5a3-4e87-bec6-3648285cdc34 |    2 | Bob          | Keller      | Bern   | bob.keller@example.com   | D28A23C8422275E006FCF3D86AA51CF4E058FB495B8E48560FC9BF7BCC019B40 | 2026-01-05 00:00:00 | ACTIVE   | INSERT_NEW_VERSION | CASE_1      | 2026-01-05 00:00:00 | 9999-12-31 23:59:59 | True           | True           |
|                   | f5d2b1a2-45cf-456c-8454-73f70b3d0df7 |    3 | Clara        | Schmid      | Basel  | clara.schmid@example.com | 77C069EE2AA3730894A6E3319ADC455C203B6CC4D35B0B912C2FAADF3C687676 | 2026-01-05 00:00:00 | ACTIVE   | INSERT_NEW_VERSION | CASE_1      | 2026-01-05 00:00:00 | 9999-12-31 23:59:59 | True           | True           |



**Dimensional Table `dim_person`**


| dp_record_id                                                            | id                                   | first_name                               | last_name                                 | city                                      | email                                                       | dp_ts_from                                             | dp_ts_to                                               | dp_is_active                            | dp_is_latest                            | dp_created_at                                          | dp_replaced_at                                         |
|-------------------------------------------------------------------------|--------------------------------------|------------------------------------------|-------------------------------------------|-------------------------------------------|-------------------------------------------------------------|--------------------------------------------------------|--------------------------------------------------------|-----------------------------------------|-----------------------------------------|--------------------------------------------------------|--------------------------------------------------------|
| <span style='color: green;'>52eee26e-08d0-413b-adbf-791e05b733c5</span> | <span style='color: green;'>1</span> | <span style='color: green;'>Alice</span> | <span style='color: green;'>Meyer</span>  | <span style='color: green;'>Zurich</span> | <span style='color: green;'>alice.meyer@example.com</span>  | <span style='color: green;'>2026-01-05 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-06 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>9a02a743-ef32-4033-a084-5ad3cdfdbbdb</span> | <span style='color: green;'>2</span> | <span style='color: green;'>Bob</span>   | <span style='color: green;'>Keller</span> | <span style='color: green;'>Bern</span>   | <span style='color: green;'>bob.keller@example.com</span>   | <span style='color: green;'>2026-01-05 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-06 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| <span style='color: green;'>3f5d237f-89c0-4b59-89b1-0641f31fdfaa</span> | <span style='color: green;'>3</span> | <span style='color: green;'>Clara</span> | <span style='color: green;'>Schmid</span> | <span style='color: green;'>Basel</span>  | <span style='color: green;'>clara.schmid@example.com</span> | <span style='color: green;'>2026-01-05 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> | <span style='color: green;'>True</span> | <span style='color: green;'>True</span> | <span style='color: green;'>2026-01-06 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |

_the following columns where excluded from the result: `dp_record_hash, dp_load_timestamp, change_type`_

## Test Step 2
At 2026-01-10 00:00:00, insert the entity with `id=1` with a different value for `city` into the new partitions of the raw table and perform SCD2 merge.


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


| merge_record_id   | dp_record_id                         |   id | first_name   | last_name   | city   | email                   | dp_record_hash                                                   | dp_ts               | status   | operation_type     | case_name   | dp_ts_from          | dp_ts_to            | dp_is_active   | dp_is_latest   |
|-------------------|--------------------------------------|------|--------------|-------------|--------|-------------------------|------------------------------------------------------------------|---------------------|----------|--------------------|-------------|---------------------|---------------------|----------------|----------------|
|                   | c56a4405-6174-4f39-8c08-ccb0ebefc806 |    1 | Alice        | Meyer       | Geneva | alice.meyer@example.com | 78FFEBE2007761B0577842A5487D3B5327964306AA3FE61834D60464D0D4AF8F | 2026-01-10 00:00:00 | ACTIVE   | INSERT_NEW_VERSION | CASE_19     | 2026-01-01 00:00:00 | 2026-01-04 23:59:59 | False          | False          |



**Dimensional Table `dim_person`**


| dp_record_id                                                            | id                                   | first_name                               | last_name                                | city                                      | email                                                      | dp_ts_from                                             | dp_ts_to                                               | dp_is_active                             | dp_is_latest                             | dp_created_at                                          | dp_replaced_at                                         |
|-------------------------------------------------------------------------|--------------------------------------|------------------------------------------|------------------------------------------|-------------------------------------------|------------------------------------------------------------|--------------------------------------------------------|--------------------------------------------------------|------------------------------------------|------------------------------------------|--------------------------------------------------------|--------------------------------------------------------|
| <span style='color: green;'>d8eceae8-4d45-4014-9904-600a64afae99</span> | <span style='color: green;'>1</span> | <span style='color: green;'>Alice</span> | <span style='color: green;'>Meyer</span> | <span style='color: green;'>Geneva</span> | <span style='color: green;'>alice.meyer@example.com</span> | <span style='color: green;'>2026-01-01 00:00:00</span> | <span style='color: green;'>2026-01-04 23:59:59</span> | <span style='color: green;'>False</span> | <span style='color: green;'>False</span> | <span style='color: green;'>2026-01-11 00:00:00</span> | <span style='color: green;'>9999-12-31 23:59:59</span> |
| 52eee26e-08d0-413b-adbf-791e05b733c5                                    | 1                                    | Alice                                    | Meyer                                    | Zurich                                    | alice.meyer@example.com                                    | 2026-01-05 00:00:00                                    | 9999-12-31 23:59:59                                    | True                                     | True                                     | 2026-01-06 00:00:00                                    | 9999-12-31 23:59:59                                    |
| 9a02a743-ef32-4033-a084-5ad3cdfdbbdb                                    | 2                                    | Bob                                      | Keller                                   | Bern                                      | bob.keller@example.com                                     | 2026-01-05 00:00:00                                    | 9999-12-31 23:59:59                                    | True                                     | True                                     | 2026-01-06 00:00:00                                    | 9999-12-31 23:59:59                                    |
| 3f5d237f-89c0-4b59-89b1-0641f31fdfaa                                    | 3                                    | Clara                                    | Schmid                                   | Basel                                     | clara.schmid@example.com                                   | 2026-01-05 00:00:00                                    | 9999-12-31 23:59:59                                    | True                                     | True                                     | 2026-01-06 00:00:00                                    | 9999-12-31 23:59:59                                    |

_the following columns where excluded from the result: `dp_record_hash, dp_load_timestamp, change_type`_

